#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>
#include <cstdio>
#include <csignal>
#include <iostream>
#include <sys/select.h>
#include <string>
#include <fcntl.h>
#include <vector>
#include <unistd.h>
#include <algorithm>

constexpr int PORT = 5555;
constexpr int MAXBUFF = 1024 * 1024;

struct Client
{
    int fd_client = -1;
    std::string buff;
    bool alive = true;
};

bool set_noblocking(int fd)
{
    int fd_flags = fcntl(fd, F_GETFL, 0);
    if (fd_flags < 0)
    {
        perror("fcntl(F_GETFL)");
        return false;
    }
    if (fcntl(fd, F_SETFL, fd_flags | O_NONBLOCK) < 0)
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
        ssize_t n = send(c.fd_client, c.buff.data(), c.buff.size(), 0);

        if (n > 0)
        {
            c.buff.erase(0, static_cast<size_t>(n));
            continue;
        }
        else if (n == 0)
        {
            break;
        }
        else if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }

            c.alive = false;
            perror("fatal error send");
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
        perror("set_noblocking(O_NONBLOCK)");
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
    std::vector<Client> clients;

    while (true)
    {
        fd_set readfd;
        fd_set writefd;

        FD_ZERO(&readfd);
        FD_ZERO(&writefd);

        int max_fd = -1;

        if (fd_lst < FD_SETSIZE)
        {
            FD_SET(fd_lst, &readfd);
            max_fd = std::max(max_fd, fd_lst);
        }

        for (Client &c : clients)
        {
            if (c.alive && !c.buff.empty())
            {
                FD_SET(c.fd_client, &writefd);
            }
            if (c.alive && c.buff.size() < MAXBUFF)
            {
                FD_SET(c.fd_client, &readfd);
            }
            if (max_fd < c.fd_client)
            {
                max_fd = c.fd_client;
            }
        }

        int active = select(max_fd + 1, &readfd, &writefd, nullptr, nullptr);
        if (active < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("select");
            break;
        }

        if (FD_ISSET(fd_lst, &readfd))
        {
            while (true)
            {
                int fd_accept = accept(fd_lst, nullptr, nullptr);
                if (fd_accept < 0)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }
                    else if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        break;
                    }
                    else if (errno == ECONNRESET || errno == ECONNABORTED || errno == EPIPE)
                    {
                        continue;
                    }
                    perror("accept");
                    break;
                }
                if (fd_accept >= FD_SETSIZE)
                {
                    close(fd_accept);
                    break;
                }
                if (!set_noblocking(fd_accept))
                {
                    close(fd_accept);
                    continue;
                }
                Client cl{};
                cl.fd_client = fd_accept;
                clients.push_back(cl);
            }
        }

        for (Client &c : clients)
        {
            if (FD_ISSET(c.fd_client, &readfd))
            {
                ssize_t n = recv(c.fd_client, buff, sizeof(buff), 0);
                if (n > 0)
                {
                    c.buff.append(buff, static_cast<size_t>(n));
                    flush_out(c);
                }
                else if (n == 0)
                {
                    c.alive = false;
                    continue;
                }
                else if (n < 0)
                {
                    if (errno == ECONNRESET || errno == ECONNABORTED)
                    {
                        c.alive = false;
                        continue;
                    }
                    else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    {
                        continue;
                    }

                    c.alive = false;
                    continue;
                }
            }
            if (FD_ISSET(c.fd_client, &writefd))
            {
                if (c.alive)
                {
                    flush_out(c);
                }
            }
                }   
        clients.erase(std::remove_if(clients.begin(), clients.end(), [](Client &c)
        {
            if (c.alive) return false;
            if (c.fd_client >= 0) close(c.fd_client);
            return true;
        }), clients.end());

    }   

    
    for (Client &c : clients)
    {
        if (c.fd_client >= 0)
            close(c.fd_client);
    }
    close(fd_lst);
    return 0;
}
