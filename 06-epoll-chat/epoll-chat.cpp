#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <csignal>
#include <iostream>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <vector>
#include <algorithm>
#include <cstring>

constexpr int MAXCLIENTS = 1024;
constexpr int MAXEVENT = 64;
constexpr int PORT = 5555;
constexpr int MAXBUF = 1024 * 1024;

struct Client
{
    int fd_client = -1;
    uint32_t event = 0;
    std::string inbuff{};
    std::string outbuff{};
    bool alive = true;
    bool FIN = false;
    int sent = 0;
};

void handle_message(Client * c, std::string* data, size_t size){
    uint32_t be = htons(size);
    const char * str = reinterpret_cast<const char *>(&be);
    c->outbuff.insert(c->outbuff.end(), str, str + 4);
    c->outbuff.insert(c->outbuff.end(), data, data + size);
}

bool set_noblocking(int fd)
{
    int flag = fcntl(fd, F_GETFL, 0);
    if (flag < 0)
    {
        perror("error fcntl(F_GETFL)");
        return false;
    }
    if (fcntl(fd, F_SETFL, flag | O_NONBLOCK) < 0)
    {
        perror("fcntl(F_SETFL(O_NONBLOCK))");
        return false;
    }
    return true;
}
void close_client(int ec, Client *c, int &nclients)
{
    if (epoll_ctl(ec, EPOLL_CTL_DEL, c->fd_client, nullptr))
    {
        perror("error EPOLL_CTL_DEL");
    }
    close(c->fd_client);
    delete c;
    --nclients;
}
void flush_out(Client *c, Client *from)
{

    while (from->sent < c->inbuff.size())
    {
        ssize_t n = send(from->fd_client, c->inbuff.data() + from->sent, c->inbuff.size() - from->sent, 0);
        if (n > 0)
        {
            from->sent += n;
            continue;
        }
        else if (n < 0)
        {
            if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
            {
                from->alive = false;
                break;
            }
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            else if (errno == EINTR)
            {
                continue;
            }
            c->alive = false;
            break;
        }
        else
        {
            c->alive = false;
            break;
        }
    }
}

bool handle_event(int ec, Client *c)
{
    uint32_t events = EPOLLET;
    if (!c->inbuff.empty())
    {
        events |= EPOLLOUT;
    }
    if (!c->FIN && c->inbuff.size() < MAXBUF)
    {
        events |= EPOLLIN;
    }

    if (c->event == events)
    {
        return true;
    }

    epoll_event eevents{};
    eevents.data.ptr = c;
    eevents.events = events;

    if (epoll_ctl(ec, EPOLL_CTL_MOD, c->fd_client, &eevents) < 0)
    {
        perror("error epoll_ctl(EPOLL_CTL_MOD)");
        return false;
    }
    c->event = events;
    return true;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    int fd_lst = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_lst < 0)
    {
        perror("socket(fd_lst)");
        return 1;
    }

    int reuse = 1;

    if (setsockopt(fd_lst, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        perror("error setsockopt(SO_REUSEADDR)");
    }

    if (!set_noblocking(fd_lst))
    {
        perror("fatal error noblocking");
        close(fd_lst);
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(fd_lst, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("error bind");
        close(fd_lst);
        return 1;
    }

    if (listen(fd_lst, SOMAXCONN) < 0)
    {
        perror("listen");
        close(fd_lst);
        return 1;
    }

    int nclients = 0;
    char buff[4096];
    int ec = epoll_create1(0);
    if (ec < 0)
    {
        perror("error epoll_create1");
        close(fd_lst);
        return 1;
    }
    epoll_event levent{};
    levent.events = EPOLLIN | EPOLLET;
    levent.data.ptr = nullptr;
    epoll_event events[MAXEVENT];
    std::vector<Client *> clients;

    if (epoll_ctl(ec, EPOLL_CTL_ADD, fd_lst, &levent) < 0)
    {
        perror("error EPOLL_CTL_ADD(fd_lst)");
        close(fd_lst);
        close(ec);
        return 1;
    }

    while (true)
    {
        int active = epoll_wait(ec, events, MAXEVENT, -1);
        if (active < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("fatal error epoll_wait");
            break;
        }
        for (int i = 0; i < active; i++)
        {
            if (events[i].data.ptr == nullptr)
            {
                while (true)
                {
                    int fd_client = accept(fd_lst, nullptr, nullptr);
                    if (fd_client < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            break;
                        }
                        else if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
                        {
                            continue;
                        }
                        else if (errno == EINTR)
                        {
                            continue;
                        }
                        perror("fatal error accept");
                        break;
                    }
                    if (!set_noblocking(fd_client) || nclients >= MAXCLIENTS)
                    {
                        close(fd_client);
                        continue;
                    }
                    Client *c = new Client;
                    c->fd_client = fd_client;
                    c->event = EPOLLIN | EPOLLET;
                    epoll_event clientevent{};
                    clientevent.data.ptr = c;
                    clientevent.events = c->event;
                    if (epoll_ctl(ec, EPOLL_CTL_ADD, c->fd_client, &clientevent) < 0)
                    {
                        perror("epoll_ctl(ADD)");
                        close(fd_client);
                        delete c;
                        continue;
                    }
                    ++nclients;
                    std::cout << "------------------------------\n";
                    std::cout << "[Client " << fd_client << "] connected\n";
                    std::cout << "------------------------------\n";
                    for (std::size_t i = 0; i < 2; i++)
                    {
                        std::cout << '\n';
                    }
                    clients.push_back(c);
                    for (int i = 0; i < clients.size(); i++)
                    {
                        if (fd_client == clients[i]->fd_client)
                        {
                            continue;
                        }
                        std::string name = "[Client " + std::to_string(fd_client) + "] connected\n";
                        send(clients[i]->fd_client, name.data(), name.size(), 0);
                    }
                }
            }
            else if (events[i].data.ptr != nullptr)
            {
                Client *c = static_cast<Client *>(events[i].data.ptr);
                if (events[i].events & EPOLLERR)
                {
                    c->alive = false;
                    auto it = std::find(clients.begin(), clients.end(), c);
                    clients.erase(it);
                    close_client(ec, c, nclients);
                    continue;
                }
                while (true)
                {
                    if (events[i].events & (EPOLLIN | EPOLLHUP))
                    {
                        ssize_t n = recv(c->fd_client, buff, sizeof(buff), 0);
                        if (n > 0)
                        {
                            c->inbuff.append(buff, static_cast<std::size_t>(n));
                            continue;
                        }
                        else if (n == 0)
                        {
                            c->FIN = true;
                            break;
                        }
                        else if (n < 0)
                        {
                            if (errno == EWOULDBLOCK || errno == EAGAIN)
                            {
                                break;
                            }
                            else if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
                            {
                                c->alive = false;
                                break;
                            }
                            else if (errno == EINTR)
                            {
                                continue;
                            }
                            perror("fatal error read");
                            c->alive = false;
                            break;
                        }
                    }
                }

                if (!c->inbuff.empty() && c->inbuff.front() == '\n')
                {
                    c->inbuff.pop_back();
                }

                if (!c->inbuff.empty())
                {
                    std::string temp = "[Client " + std::to_string(c->fd_client) + "]: ";
                    for (int i = 0; i < clients.size(); i++)
                    {
                        std::string out;
                        if (c->fd_client == clients[i]->fd_client)
                        {
                            out = "\033[A\033[2K\r" + temp;
                            send(clients[i]->fd_client, out.data(), out.size(), 0);
                            flush_out(c, clients[i]);
                            continue;
                        }
                        out = temp;
                        send(clients[i]->fd_client, out.data(), out.size(), 0);
                        flush_out(c, clients[i]);
                    }
                }
                c->inbuff.clear();
                for (int i = 0; i < clients.size(); i++)
                {
                    clients[i]->sent = 0;
                }

                if (c->FIN || !c->alive)
                {
                    std::string tempcl = "[Client " + std::to_string(c->fd_client) + "] disconnected\n";
                    for (Client *clie : clients)
                    {
                        if (clie->fd_client == c->fd_client)
                        {
                            continue;
                        }
                        send(clie->fd_client, tempcl.data(), tempcl.size(), 0);
                    }
                    std::cout << "[Client " << c->fd_client << "] disconnected\n";
                    close_client(ec, c, nclients);
                    auto it = std::find(clients.begin(), clients.end(), c);
                    clients.erase(it);
                    continue;
                }
            }
        }
    }

    close(ec);
    close(fd_lst);
    return 0;
}