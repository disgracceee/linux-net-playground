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
constexpr int MAXCLIENTS = 1024;
constexpr int MAXEVENT = 64;
constexpr int PORT = 5555;
constexpr int MAXBUF = 1024 * 1024;

struct Client
{
    int fd_clients = -1;
    std::string buff{};
    uint32_t event = 0;
    bool alive = true;
};

bool set_noblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, nullptr);
    if (flags < 0)
    {
        perror("fcntl(F_GETFL)");
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        perror("fcntl(F_SETFL)");
        return false;
    }
    return true;
}

void flush_out(Client &c)
{
    while (!c.buff.empty())
    {
        ssize_t n = send(c.fd_clients, c.buff.data(), c.buff.size(), 0);
        if (n > 0)
        {
            c.buff.erase(0, static_cast<size_t>(n));
            continue;
        }
        else if (n < 0)
        {
            if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
            {
                c.alive = false;
                break;
            }
            else if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                break;
            }
            else if (errno == EINTR)
            {
                continue;
            }
            else
            {
                perror("fatal error send");
                c.alive = false;
                break;
            }
        }
    }
}
void close_client(int ep, Client *c, int &nclients)
{
    if (epoll_ctl(ep, EPOLL_CTL_DEL, c->fd_clients, nullptr) < 0)
    {
        perror("error EPOLL_CTL_DEL");
    }
    close(c->fd_clients);
    delete c;
    --nclients;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    int fd_lst = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_lst < 0)
    {
        perror("socket(lst)");
        return 1;
    }

    sockaddr_in addr{};

    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);

    int reuse = 1;
    if (setsockopt(fd_lst, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        perror("setsockopt(SO_REUSEADDR)");
    }

    if (!set_noblocking(fd_lst))
    {
        close(fd_lst);
        return 1;
    }

    if (bind(fd_lst, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("bind");
        close(fd_lst);
        return 1;
    }

    if (listen(fd_lst, SOMAXCONN) < 0)
    {
        perror("listen");
        close(fd_lst);
        return 1;
    }

    char buff[4096]{};
    int ec = epoll_create1(0);
    if (ec < 0)
    {
        perror("epoll_create");
        close(fd_lst);
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = nullptr;
    if (epoll_ctl(ec, EPOLL_CTL_ADD, fd_lst, &ev) < 0)
    {
        perror("epoll_ctl(listen)");
        close(fd_lst);
        close(ec);
        return 1;
    }

    epoll_event events[MAXEVENT];
    int nclients = 0;

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
                    int fd_clients = accept(fd_lst, nullptr, nullptr);
                    if (fd_clients < 0)
                    {
                        if (errno == EINTR)
                        {
                            continue;
                        }
                        else if (errno == ECONNABORTED || errno == ECONNRESET)
                        {
                            continue;
                        }
                        else if (errno == EWOULDBLOCK || errno == EAGAIN)
                        {
                            break;
                        }
                        perror("fatal error accept");
                        break;
                    }

                    if (!set_noblocking(fd_clients) || nclients >= MAXCLIENTS)
                    {
                        close(fd_clients);
                        continue;
                    }
                    Client *c = new Client;
                    c->event |= EPOLLIN | EPOLLOUT | EPOLLET;
                    c->fd_clients = fd_clients;
                    epoll_event eventcli{};
                    eventcli.data.ptr = c;
                    eventcli.events = c->event;
                    if (epoll_ctl(ec, EPOLL_CTL_ADD, c->fd_clients, &eventcli) < 0)
                    {
                        perror("error epoll_ctl(EPOLL_STL_ADD)");
                        close(fd_clients);
                        delete (c);
                        continue;
                    }
                    std::cerr << "[Client " << c->fd_clients << " " << " ] connected\n";
                    ++nclients;
                }
            }
            else if (events[i].data.ptr != nullptr)
            {
                Client *c = static_cast<Client *>(events[i].data.ptr);
                if (events[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLOUT))
                {
                    if (events[i].events & (EPOLLERR | EPOLLHUP))
                    {
                        c->alive = false;
                        close_client(ec, c, nclients);
                        continue;
                    }
                    while (true)
                    {
                        ssize_t n = recv(c->fd_clients, buff, sizeof(buff), 0);
                        if (n > 0)
                        {
                            c->buff.append(buff, static_cast<size_t>(n));
                        }
                        else if (n == 0)
                        {
                            c->alive = false;
                            break;
                        }
                        else if (n < 0)
                        {
                            if (errno == EWOULDBLOCK || errno == EAGAIN)
                            {
                                break;
                            }
                            else if (errno == EINTR)
                            {
                                continue;
                            }
                            else if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
                            {
                                std::cerr << "Client RST\n";
                                c->alive = false;
                                break;
                            }
                            else
                            {
                                perror("fatal error recv");
                                c->alive = false;
                                break;
                            }
                        }
                    }
                }
                if (c->alive && events[i].events & (EPOLLOUT | EPOLLIN))
                {
                    flush_out(*c);
                }
                if (!c->alive)
                {
                    std::cerr << "[Client " << c->fd_clients << " " << " ] disconnected\n";
                    close_client(ec, c, nclients);
                }
            }
        }
    }
    close(ec);
    close(fd_lst);
    return 0;
}