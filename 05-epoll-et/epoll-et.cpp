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
    int fd_client = -1;
    uint32_t event = 0;
    std::string buff{};
    bool alive = true;
    bool FIN = false;
};

bool set_noblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        perror("error fcntl(F_GETFD)");
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        perror("eror fcntl(F_SETFD(O_NONBLOCK))");
        return false;
    }
    return true;
}

bool handle_event(int ec, Client *c)
{
    uint32_t events = EPOLLET;
    if (!c->buff.empty())
    {
        events |= EPOLLOUT;
    }
    if (!c->FIN && c->buff.size() < MAXBUF)
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
void close_client(int ec, Client *c, int &nclients)
{
    if (epoll_ctl(ec, EPOLL_CTL_DEL, c->fd_client, nullptr) < 0)
    {
        perror("error epoll_ctl(DEL)");
    }
    close(c->fd_client);
    delete c;
    --nclients;
}

void flush_out(Client *c)
{
    while (!c->buff.empty())
    {
        ssize_t n = send(c->fd_client, c->buff.data(), c->buff.size(), 0);
        if (n > 0)
        {
            c->buff.erase(0, static_cast<std::size_t>(n));
            continue;
        }
        else if (n < 0)
        {
            if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
            {
                c->alive = false;
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

int main()
{
    signal(SIGPIPE, SIG_IGN);

    int fd_lst = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_lst < 0)
    {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    if (setsockopt(fd_lst, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        perror("setsockopt(REUSE)");
    }
    if (!set_noblocking(fd_lst))
    {
        close(fd_lst);
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);

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
    char buff[4096];
    int ec = epoll_create1(0);
    if (ec < 0)
    {
        perror("epoll_create1");
        close(fd_lst);
        return 1;
    }

    epoll_event eventsls;
    eventsls.events = EPOLLIN | EPOLLET;
    eventsls.data.ptr = nullptr;
    if (epoll_ctl(ec, EPOLL_CTL_ADD, fd_lst, &eventsls) < 0)
    {
        perror("epoll_ctl(ADD(fd_lst))");
        close(fd_lst);
        close(ec);
        return 1;
    }

    epoll_event events[MAXEVENT]{};
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
                    int fd_client = accept(fd_lst, nullptr, nullptr);
                    if (fd_client < 0)
                    {
                        if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
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
                    if (!set_noblocking(fd_client) || nclients >= MAXCLIENTS)
                    {
                        close(fd_client);
                        continue;
                    }
                    Client *c = new Client;
                    c->fd_client = fd_client;
                    c->event = EPOLLET | EPOLLIN;
                    epoll_event cevent{};
                    cevent.data.ptr = c;
                    cevent.events = c->event;
                    if (epoll_ctl(ec, EPOLL_CTL_ADD, fd_client, &cevent) < 0)
                    {
                        perror("epoll_ctl(accept)");
                        close(fd_client);
                        delete c;
                        continue;
                    }
                    std::cout << "[Client " << fd_client << "] connected\n";
                    ++nclients;
                }
            }
            else if (events[i].data.ptr != nullptr)
            {
                Client *c = static_cast<Client *>(events[i].data.ptr);
                if (events[i].events & EPOLLERR)
                {
                    c->alive = false;
                }
                else if (events[i].events & (EPOLLIN | EPOLLHUP))
                {
                    while (true)
                    {
                        ssize_t n = recv(c->fd_client, buff, sizeof(buff), 0);
                        if (n > 0)
                        {
                            c->buff.append(buff, static_cast<std::size_t>(n));
                            continue;
                        }
                        else if (n < 0)
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                            {
                                break;
                            }
                            else if (errno == EINTR)
                            {
                                continue;
                            }
                            else if (errno == ECONNRESET || errno == ECONNABORTED || errno == EPIPE)
                            {
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
                        else if (n == 0)
                        {
                            c->FIN = true;
                            break;
                        }
                    }
                }
                if (c->alive)
                    flush_out(c);

                if (!c->alive || (c->FIN && c->buff.empty()))
                {
                    std::cerr << "[Client " << c->fd_client << "] disconnected\n";
                    close_client(ec, c, nclients);
                    continue;
                }

                if(!handle_event(ec, c)){
                    close_client(ec,c,nclients);
                }
            }
        }
    }
    close(fd_lst);
    close(ec);
    return 1;
}
