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
constexpr int MAX_PAYLOUT = 1 * 1024 * 1024;
constexpr int MAX_INBUF = 4 + MAX_PAYLOUT + 64 * 1024;

struct Client
{
    int fd_client = -1;
    uint32_t event = 0;
    std::string inbuff{};
    std::string outbuff{};
    bool alive = true;
    bool FIN = false;
};

std::vector<Client *> clients;
std::vector<Client *> deadclients;

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

bool update_events(int ec, Client *c)
{
    uint32_t mask = EPOLLET;
    if (c->alive && !c->FIN && c->inbuff.size() < MAX_INBUF)
    {
        mask |= EPOLLIN;
    }
    if (!c->outbuff.empty())
    {
        mask |= EPOLLOUT;
    }

    if (c->event == mask)
    {
        return true;
    }

    epoll_event eevent{};
    eevent.data.ptr = c;
    eevent.events = mask;
    if (epoll_ctl(ec, EPOLL_CTL_MOD, c->fd_client, &eevent) < 0)
    {
        perror("epoll_ctl(MOD)");
        return false;
    }
    c->event = mask;
    return true;
}

bool on_data(Client *c, int ec)
{
    int offset = 0;
    while (true)
    {
        int available = c->inbuff.size() - offset;
        if (available < 4)
        {
            break;
        }

        uint32_t be = 0;
        memcpy(&be, c->inbuff.data() + offset, 4);
        uint32_t len = ntohl(be);

        if (len > MAX_PAYLOUT)
        {
            return false;
        }
        if (available - 4 < static_cast<int>(len))
        {
            break;
        }
        std::string msg(c->inbuff.data() + offset + 4, len);
        std::string msg_pref_client = "[Client " + std::to_string(c->fd_client) + "]: " + msg + '\n';
        uint32_t new_len_le = msg_pref_client.size();
        uint32_t new_len_be = htonl(new_len_le);
        const char *ptr = reinterpret_cast<const char *>(&new_len_be);
        for (Client *cl : clients)
        {
            if (cl == c || cl->alive == false)
            {
                continue;
            }
            if (cl->outbuff.size() + static_cast<std::size_t>(new_len_le) + 4 > MAX_INBUF)
            {
                cl->alive = false;
                continue;
            }
            cl->outbuff.append(ptr, 4);
            cl->outbuff.append(msg_pref_client);
            if (!update_events(ec, cl))
            {
                cl->alive = false;
            }
        }
        offset += len + 4;
    }
    if (offset)
    {
        c->inbuff.erase(0, offset);
    }
    return true;
}
void flush_out(Client *to)
{
    while (!to->outbuff.empty())
    {
        ssize_t n = send(to->fd_client, to->outbuff.data(), to->outbuff.size(), 0);
        if (n > 0)
        {
            to->outbuff.erase(0, static_cast<std::size_t>(n));
            continue;
        }
        else if (n < 0)
        {
            if (errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE)
            {
                to->alive = false;
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
            else
            {
                perror("error send");
                to->alive = false;
                break;
            }
        }
    }
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
                    std::string msg = "[Client " + std::to_string(c->fd_client) + "] connected\n";
                    uint32_t size = htonl(msg.size());
                    for (Client *cj : clients)
                    {
                        if (cj == c)
                        {
                            std::string hello = "Welcome, you’ve connected.\n";
                            uint32_t sizeh = htonl(hello.size());
                            const char *ptr = reinterpret_cast<const char *>(&sizeh);
                            c->outbuff.append(ptr, 4);
                            c->outbuff.append(hello);
                            if (!update_events(ec, c))
                            {
                                cj->alive = false;
                            }
                            continue;
                        }
                        const char *p = reinterpret_cast<const char *>(&size);
                        if (cj->outbuff.size() + msg.size() + 4 > MAX_INBUF)
                        {
                            cj->alive = false;
                            continue;
                        }
                        cj->outbuff.append(p, 4);
                        cj->outbuff.append(msg);

                        if (!update_events(ec, cj))
                        {
                            cj->alive = false;
                            continue;
                        }
                    }
                }
            }
            else if (events[i].data.ptr != nullptr)
            {
                Client *c = static_cast<Client *>(events[i].data.ptr);
                if (!c->alive)
                {
                    continue;
                }
                if (events[i].events & EPOLLERR)
                {
                    c->alive = false;
                    continue;
                }
                if (events[i].events & (EPOLLIN | EPOLLHUP))
                {
                    while (true)
                    {
                        ssize_t n = recv(c->fd_client, buff, sizeof(buff), 0);
                        if (n > 0)
                        {
                            if (c->inbuff.size() + n > MAX_INBUF)
                            {
                                c->alive = false;
                                break;
                            }
                            c->inbuff.append(buff, static_cast<std::size_t>(n));
                            if (!on_data(c, ec))
                            {
                                c->alive = false;
                                break;
                            }
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
                if (c->alive)
                {
                    flush_out(c);
                }
                if (c->alive && c->FIN && !c->outbuff.empty())
                {
                    flush_out(c);
                }
                if (c->alive && c->FIN && c->outbuff.empty())
                {
                    c->alive = false;
                }
                if (c->alive && !update_events(ec, c))
                {
                    c->alive = false;
                }
            }
        }
        bool any_dead = true;
        while (any_dead)
        {
            any_dead = false;
            for (auto it = clients.begin(); it != clients.end();)
            {
                if (!(*it)->alive)
                {
                    deadclients.push_back(*it);
                    it = clients.erase(it);
                }
                else
                {
                    it++;
                }
            }
            for (Client *dead_client : deadclients)
            {
                std::string msg_disconnected_for_all_live = "[Client " + std::to_string(dead_client->fd_client) + "] disconnected\n";
                uint32_t len_be = htonl(msg_disconnected_for_all_live.size());
                const char *ptr = reinterpret_cast<const char *>(&len_be);
                for (Client *live_client : clients)
                {
                    if (!live_client->alive)
                    {
                        continue;
                    }
                    if (live_client->outbuff.size() + msg_disconnected_for_all_live.size() + 4 > MAX_INBUF)
                    {
                        live_client->alive = false;
                        any_dead = true;
                        continue;
                    }
                    live_client->outbuff.append(ptr, 4);
                    live_client->outbuff.append(msg_disconnected_for_all_live);
                    if (!update_events(ec, live_client))
                    {
                        any_dead = true;
                        live_client->alive = false;
                        continue;
                    }
                }
            }

            for (Client *dead_client : deadclients)
            {
                std::cout << "[Client " << dead_client->fd_client << "] disconnected\n";
                close_client(ec, dead_client, nclients);
            }
            deadclients.clear();
        }
    }
    close(fd_lst);
    close(ec);
    return 0;
}
