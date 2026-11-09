#include <iostream>
#include <string>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 5555


bool recv_all(int sock, char* buffer, size_t length) {
    size_t total_received = 0;
    while (total_received < length) {
        ssize_t n = recv(sock, buffer + total_received, length - total_received, 0);
        if (n <= 0) {
            return false; 
        }
        total_received += n;
    }
    return true;
}


void receive_thread(int sock) {
    while (true) {
        uint32_t len_net = 0;
       
        if (!recv_all(sock, reinterpret_cast<char*>(&len_net), 4)) {
            std::cout << "\n[Соединение с сервером потеряно]" << std::endl;
            break;
        }

       
        uint32_t len = ntohl(len_net);

        
        if (len > 1024 * 1024) {
            std::cout << "\n[Ошибка: Сервер прислал слишком большое сообщение]" << std::endl;
            break;
        }

       
        std::string msg(len, '\0');
        if (!recv_all(sock, msg.data(), len)) {
            std::cout << "\n[Ошибка чтения сообщения]" << std::endl;
            break;
        }

        
        std::cout << msg; 
       
        if (msg.back() != '\n') std::cout << std::endl;
    }
}

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        return 1;
    }

    
    if (connect(sock, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        perror("connect");
        return 1;
    }

    std::cout << "Connected to chat server " << SERVER_IP << ":" << SERVER_PORT << std::endl;

    
    std::thread recv_thread(receive_thread, sock);
    recv_thread.detach(); 

   
    std::string input;
    while (true) {
        std::getline(std::cin, input);
        
        if (input == "exit" || input == "quit") {
            break;
        }
        if (input.empty()) continue;

        
        uint32_t len_net = htonl(input.size());
        
        
        if (send(sock, &len_net, 4, 0) < 0) break;
        
        if (send(sock, input.data(), input.size(), 0) < 0) break;
    }

    close(sock);
    std::cout << "Вы вышли из чата." << std::endl;
    return 0;
}