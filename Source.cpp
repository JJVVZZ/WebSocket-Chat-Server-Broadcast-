#include<iostream>
#include<boost/asio.hpp>
#include<windows.h>
#include<boost/beast/core.hpp>
#include<boost/beast/websocket.hpp>
#include <algorithm> 
#include <memory>             
#include <string>          
#include <vector>  
#include<thread>
#include<queue>
#pragma execution_character_set("utf-8")
using namespace std;

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;



class Session : public enable_shared_from_this<Session>
{
	
	beast::flat_buffer read_buffer_;
	bool is_writing = false;
	websocket::stream<tcp::socket> socket_;
	string nickname = "Guest";
	queue<shared_ptr<string const>> write_queue_;

	static vector<weak_ptr<Session>> all_clients_;
	static mutex client_mutex_;

	void do_read()
	{
		auto self = shared_from_this();
		socket_.async_read(read_buffer_, [self](beast::error_code ec, size_t bytes_transferred)
			{
				if (ec)
				{
					if (ec == websocket::error::closed)
					{
						std::cout << self->nickname << " closed the connection" << std::endl;
					}
					else
					{
						cerr << "Read error" << ec.message() << endl;
					}
					return;
				}
				
				string message = beast::buffers_to_string(self->read_buffer_.data());

				self->read_buffer_.consume(self->read_buffer_.size());

				while (!message.empty() && (message.back() == '\n' || message.back() == '\r'))
				{
					message.pop_back();
				}
				if (!message.empty())
				{
					std::cout << "[" << self->nickname << "] " << message << std::endl;

					if (message.size() >= 6 && message.substr(0, 6) == "/nick ")
					{
						string new_nick = message.substr(6);
						if (!new_nick.empty())
						{
							string old = self->nickname;
							self->nickname = new_nick;

							self->do_write("Your nickname has been changed to: " + new_nick + "\n");
							self->broadcast(old + " Now " + new_nick + "\n");
						}
						else
						{
							self->do_write("Error: /nick requires a name\n");
						}
					}
					else if (message == "/users")
					{
						string users = "In the chat " + to_string(count_clients()) + " Human:\n";
						{
							lock_guard<mutex> lock(client_mutex_);
							for (const auto& wp : all_clients_)
							{
								if (auto client = wp.lock())
								{
									users += "- " + client->nickname + "\n";
								}
							}
						}
						self->do_write(users);
						
					}
					else if (message == "/exit" || message == "/quit")
					{
						self->close();		
						return;
					}
					else
					{
						self->broadcast(message + "\n");
					}
					self->do_read();
				}

			});
		
	}
	void do_write(const string& message)
	{

		auto msg = make_shared<const string>(message + "\n");

		write_queue_.push(msg);

		if (is_writing)
			return;


		do_write_next();
	}
	void do_write_next()
	{

		if (write_queue_.empty())
		{
			is_writing = false;
			return;
		}
		is_writing = true;
		auto temp = write_queue_.front();
		write_queue_.pop();

		auto self = shared_from_this();
		socket_.text(true);
		socket_.async_write(net::buffer(*temp), [self,temp](beast::error_code ec, size_t)
			{

				if (ec)
				{
					std::cerr << "Error sending to self:" << ec.message() << std::endl;

					if (ec != websocket::error::closed)
					{
						cerr << "Sending error:" << ec.message() << endl;
					}
					self->close();
					return;
				}
				self->do_write_next();
			});
	}

	void broadcast(const string& message)
	{
		string formatted = "[" + nickname + "] " + message + "\n";
		lock_guard<mutex> lock(client_mutex_);

		for (const auto& wp : all_clients_)
		{
			if (auto client = wp.lock())
			{
				if(client.get() != this)
				client->do_write(formatted);
			}
		}
	}
	void close()
	{
		broadcast(nickname + " left the chat");

		{
			lock_guard<mutex> lock(client_mutex_);
			all_clients_.erase(remove_if(all_clients_.begin(), all_clients_.end(), [this](const weak_ptr<Session>& wp) 
				{
					auto sp = wp.lock();
					return !sp || sp.get() == this;
				}),all_clients_.end());
		}
		beast::error_code ec;
		socket_.close(websocket::close_code::normal,ec);
	}

	static size_t count_clients()
	{
		lock_guard<mutex> lock(client_mutex_);
		return count_if(all_clients_.begin(), all_clients_.end(), [](const weak_ptr<Session>& wp) {return !wp.expired(); });
	}

	void do_accept()
	{
		auto self = shared_from_this();

		socket_.async_accept([self](beast::error_code ec) 
			{
				if (ec)
				{
					std::cerr << "Handshake error:" << ec.message() << std::endl;
					return;
				}

				std::cout << self->nickname << "entered the chat" << std::endl;

				self->do_write("Welcome to the chat! Your nickname:" + self->nickname +
					"\nCommands: /nick Name, /users, /exit");
				self->do_read();
			});
	}
public:
	explicit Session(tcp::socket socket) : socket_(move(socket))
	{}

	~Session()
	{
		close();
	}

	void start()
	{
		{
			lock_guard<mutex> lock(client_mutex_);
			all_clients_.push_back(weak_from_this());
		}
		cout << "Client connected:" << socket_.next_layer().remote_endpoint()
			<< " (Total clients: " << all_clients_.size() << ")" << endl;

		do_accept();
	}
};
vector<weak_ptr<Session>> Session::all_clients_;
mutex Session::client_mutex_;

class Server
{
	net::io_context& io_;
	tcp::acceptor acceptor_;

	void do_accept()
	{
		acceptor_.async_accept([this](const boost::system::error_code& ec, tcp::socket socket)
			{
				if (!ec)
				{
					make_shared<Session>(move(socket))->start();
				}
				do_accept();
			});
	}
public:
	Server(net::io_context&io, short port) : io_(io), acceptor_(io,tcp::endpoint(tcp::v4(),port))
	{
		do_accept();
	}
	void run(size_t thread_count = thread::hardware_concurrency())
	{
		vector<thread> threads;

		for (size_t i = 1; i < thread_count; ++i)
		{
			threads.emplace_back([&]() {io_.run(); });
		}
		io_.run();

		for (auto& t : threads)
			if (t.joinable()) t.join();
	}

};

int main()
{

	SetConsoleCP(65001);
	SetConsoleOutputCP(65001);
	try
	{
		net::io_context io;

		cout << "Launch the server on port 1311" << endl;

		Server server(io, 1311);
		server.run(4);
		

	}
	catch (const exception&e)
	{
		cerr << "Critical error: " << e.what() << endl;
	}
	cout << "The server has terminated.\n";
	system("pause");
	return 0;
}