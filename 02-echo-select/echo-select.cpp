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


constexpr size_t MAXBUF = 1024 * 1024;
constexpr uint16_t PORT = 5555;


bool set_noblocking(int fd){
    int fl = fcntl(fd, F_GETFL, 0);
    if(fl < 0){
        perror("fcntl(F_GETFL)");
        return false;
    }
    if(fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0){
        perror("fcntl(F_SETFL)");
        return false;
    }
    return true;
}

struct Client {
    int s = -1;
    std::string buf;
    bool alive = true;
};

void flush_out(Client& cl){
    while(!cl.buf.empty()){
        ssize_t n = send(cl.s, cl.buf.data(), cl.buf.size(), MSG_NOSIGNAL);

        if(n > 0){
            cl.buf.erase(0, static_cast<std::size_t>(n));
            continue;
        }

        if(n == 0){
            break;
        }

        if(errno == EINTR){
            continue;
        }

        if(errno == ECONNABORTED || errno == ECONNRESET || errno == EPIPE){
            cl.alive = false;
            break;
        }

        if(errno == EAGAIN || errno == EWOULDBLOCK){
            break;
        }

        std::cerr << "send: " << std::strerror(errno) << "( errno= " << errno << " )" << '\n';
        cl.alive = false;
        break;
    }
}

int main(){
    signal(SIGPIPE, SIG_IGN);

    int lst = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(lst < 0){
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    int reuse  = 1;
    if(setsockopt(lst, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0){
        perror("setsockopt");
    }

    if(bind(lst, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0){
        perror("bind");
        close(lst);
        return 1;
    }

    if(listen(lst, SOMAXCONN) < 0){
        perror("listen");
        close(lst);
        return 1;
    }

    if(!set_noblocking(lst)){
        close(lst);
        return 1;
    }

    char buff[4096];
    std::vector<Client> clients;


    while(true){
        fd_set  writefd; fd_set readfd;

        FD_ZERO(&writefd); FD_ZERO(&readfd);
        int maxfd = -1;

        if(lst < FD_SETSIZE){
            FD_SET(lst, &readfd);
            maxfd = std::max(lst, maxfd);
        }


        for(Client &c : clients){
            if(!c.buf.empty()){
                FD_SET(c.s, &writefd);
            }
            if(c.buf.size() < MAXBUF){
                FD_SET(c.s, &readfd);
            }
            if(c.s > maxfd){
                maxfd = c.s;
            }

        }

       
        int active = select(maxfd + 1, &readfd, &writefd, nullptr, nullptr);
        if(active < 0){
            if(errno == EINTR){
                continue;
            }
            perror("select");
            break;
        }


        
       if(FD_ISSET(lst, &readfd)){
        while(true){
            int client = accept(lst, nullptr, nullptr);
            if(client < 0){
                if(errno == EAGAIN || errno == EWOULDBLOCK){
                    break;
                }
                if(errno == EINTR){
                    continue;
                }
                if(errno == ECONNABORTED){
                    continue;
                }
                if(errno == ENFILE || errno == EMFILE){
                    perror("accept: fd limit");
                    break;
                }
                perror("error accept");
                break;
            }
            if(client >= FD_SETSIZE){
                close(client);
                std::cerr << "out of limit FD_SETSIZE\n";
                continue;
            }
            if(!set_noblocking(client)){
                close(client);
                continue;
            }
            Client cli;
            cli.s = client;
            clients.push_back(cli);
        }
    }

        for(Client &c : clients){
            if(!c.alive){
                continue;
            }

            if(FD_ISSET(c.s, &readfd)){
                ssize_t n = recv(c.s, buff, sizeof(buff), 0);

                if(n > 0){
                    c.buf.append(buff, static_cast<size_t>(n));
                }
                else if(n == 0){
                    c.alive = false;
                }
                else if(n < 0){
                    if(errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN){

                    } else {
                        perror("fatal error recv");
                        c.alive = false;
                    }
                }
            }

            if(c.alive){
                flush_out(c);
            }
        }
        
        // --- одна зачистка за итерацию ---
        clients.erase(
            std::remove_if(clients.begin(), clients.end(),
                [](Client& c) {
                    if (!c.alive) {
                        if (c.s >= 0) {
                            close(c.s);
                            c.s = -1;
                        }
                        return true;
                    }
                    return false;
                }),
            clients.end());
    }   // конец while

    close(lst);
    return 0;
}
