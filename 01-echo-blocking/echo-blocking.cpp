#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>
#include <cstdio>
#include <csignal>
#include <iostream>

static uint16_t PORT = 5555;

int main(){
    signal(SIGPIPE, SIG_IGN);

    int lst = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(lst < 0){
        perror("Socket:");
        close(lst);
        return 1;
    }

    sockaddr_in addr{};

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    int reuse = 1;

    if(setsockopt(lst, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0){
        std::cerr << "Error setsockopt/\n";
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

    char buf[4096];

    while(true){
        int client = accept(lst, NULL, NULL );
        if(client < 0){
            perror("Client error");
            close(client);
            break;
        }
        std::cerr << "Client connected.\n";
        while(true){
            int n = recv(client, buf, sizeof(buf), 0);
            if(n <= 0){
                std::cerr << "Error recv. Client closed.\n";
                close(client);
                break;
            }
            int sent = 0;
            while(sent < n){
                int s = send(client, buf + sent, n - sent, 0);
                if(s <= 0){
                    std::cerr << "Error send. Client closed.\n";
                    close(client);
                    break;
                }
                sent += s;
            }
        }
    }

    close(lst);
    return 0;

}