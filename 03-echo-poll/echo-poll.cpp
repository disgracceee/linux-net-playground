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
#include <poll.h>

constexpr int MAXFD = 1024;
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
    std::vector<pollfd> fds;
    fds.resize(MAXFD);

    fds[0].fd = fd_lst;
    fds[0].events = POLLIN;
    int nfds = 1;

    for (std::size_t i = 1; i < MAXFD; i++)
    {
        fds[i].fd = -1;
    }

    while(true){

        int active = poll(fds.data(), nfds, -1);
        // Проверка active
        if(active < 0){
            if(errno == EINTR){
                continue;
            }
            perror("poll fatal error");
            break;
        }
        // Проверка на новые подключение от слушающего сокета
        if(fds[0].revents & POLLIN){
            while(true){
                int fd_client = accept(fds[0].fd, nullptr, nullptr);
                if(fd_client < 0){
                    if(errno == EINTR){
                        continue;
                    }
                    else if(errno == ECONNRESET || errno == ECONNABORTED || errno == EPIPE){
                        continue;
                    }
                    else if(errno == EWOULDBLOCK || errno == EAGAIN){
                        break;
                    }
                    perror("accept");
                    break;
                }
                if(nfds >= MAXFD || !set_noblocking(fd_client)){
                    std::cerr << "error accept(nfds > MAXFD or !set_noblocking): " << errno << '\n';
                    close(fd_client);
                    continue;
                }
                Client cl;
                cl.fd_client = fd_client;
                clients.push_back(cl); 
                fds[nfds].events = POLLIN;
                fds[nfds].fd = fd_client;
                fds[nfds].revents = 0;

                nfds++;
                continue;
            }
        }
        // Обработка уже подключенных пользователей
        for(int i = nfds - 1; i >= 1; i--){
            
            Client & c = clients[i - 1];

            if(fds[i].revents & (POLLERR | POLLNVAL)) {
                c.alive = false;
            } 
            else {
                if(fds[i].revents & (POLLHUP | POLLIN)){
                    ssize_t n = recv(fds[i].fd, buff, sizeof(buff), 0);
                    if(n > 0){
                        c.buff.append(buff, static_cast<size_t>(n));
                    }
                    else if(n == 0){
                        perror("shut down client");
                        c.alive = false;
                    }
                    else if(n < 0){
                        if(errno == EINTR){
                            perror("recv");
                        }
                        else if(errno == EWOULDBLOCK || errno == EAGAIN){
                            perror("buff core if full");
                        } 
                        else {
                        perror("fatal error recv");
                        c.alive = false;
                        }
                    }
                }
            }
            if(c.alive){
                flush_out(c);
            }
            fds[i].events = 0;
            if(c.alive && c.buff.size() < static_cast<size_t>(MAXBUFF)){
                fds[i].events |= POLLIN;
            }
            if(c.alive && !c.buff.empty()){
                fds[i].events |= POLLOUT;
            }

            if (!c.alive)
            {
                std::cout << "client disconnected fd=" << fds[i].fd << "\n";
                close(fds[i].fd);

                int last = nfds - 1;
                if (i != last)
                {
                    fds[i] = fds[last];
                    clients[i - 1] = std::move(clients[last - 1]);
                }
                fds[last].fd = -1;
                fds[last].events = 0;
                fds[last].revents = 0;
                clients.pop_back();
                nfds--;
            }
        }

    }

    close(fd_lst);
    return 0;
}
