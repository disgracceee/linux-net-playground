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
#include <unordered_map>
#include <unordered_set>

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
    std::string current_room = "general";
    std::string username{};
    bool nameenter = false;
};

struct Room
{
    std::string name{};
    Client *creator = nullptr;
    std::unordered_set<Client *> clients_In{};
};
std::unordered_map<std::string, Room> rooms;
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
    rooms[c->current_room].clients_In.erase(c);
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

void send_to_client(int ec, Client *c, const std::string &data)
{

    uint32_t size_be = htonl(data.size());
    const char *ptr = reinterpret_cast<const char *>(&size_be);
    if (c->outbuff.size() + data.size() + 4 > MAX_INBUF)
    {
        c->alive = false;
        return;
    }
    c->outbuff.append(ptr, 4);
    c->outbuff.append(data);
    if (!update_events(ec, c))
    {
        c->alive = false;
        return;
    }
    return;
}

void handle_command(Client *c, std::string &msg, int ec)
{
    std::string_view word(msg);
    std::string_view tok[3];
    std::size_t i = 0;
    std::size_t ntoks = 0;

    while (i < word.size() && ntoks < 3)
    {
        while (i < word.size() && word[i] == ' ')
        {
            i++;
        }
        if (i >= word.size())
        {
            break;
        }

        std::size_t start = i;

        while (i < word.size() && word[i] != ' ')
        {
            i++;
        }
        tok[ntoks++] = word.substr(start, i - start);
    }

    if (tok[0] == "/join")
    {
        if (ntoks < 2)
        {
            send_to_client(ec, c, "You must write the name of the room\n");
            return;
        }
        else if (ntoks == 2)
        {
            std::string namerom(tok[1]);
            auto it = rooms.find(namerom);
            if (it == rooms.end())
            {
                send_to_client(ec, c, "Such a room doesn’t exist.\n");
                return;
            }
            else
            {
                if (tok[1] == c->current_room)
                {
                    send_to_client(ec, c, "You cannot connect to the room you are currently in.\n");
                    return;
                }
                else
                {
                    std::string beforeroom = c->current_room;
                    std::string namerom(tok[1]);
                    auto it_to = rooms.find(namerom);
                    auto it_from = rooms.find(beforeroom);
                    if (it_to == rooms.end())
                    {
                        send_to_client(ec, c, "Room not found\n");
                        return;
                    }
                    if (it_from == rooms.end())
                    {
                        c->current_room = namerom;
                        (*it_to).second.clients_In.insert(c);
                        send_to_client(ec, c, "You have successfully changed rooms.\n");
                        return;
                    }
                    (*it_from).second.clients_In.erase(c);
                    (*it_to).second.clients_In.insert(c);
                    c->current_room = namerom;
                    send_to_client(ec, c, "You have successfully changed rooms.\n");
                    return;
                }
            }
        }
    }
    else if (tok[0] == "/create")
    {
        if (ntoks < 2)
        {
            send_to_client(ec, c, "You need to input name room\n");
            return;
        }

        std::string newnameroom(tok[1]);
        auto it = rooms.find(newnameroom);
        if (it != rooms.end())
        {
            send_to_client(ec, c, "Room already exists. Use /join to enter it.\n");
            return;
        }

        Room newroom{};
        newroom.name = newnameroom;
        newroom.creator = c;
        rooms[newroom.name] = std::move(newroom);
        auto it2 = rooms.find(c->current_room);
        if (it2 != rooms.end())
        {
            (*it2).second.clients_In.erase(c);
        }
        rooms[newnameroom].clients_In.insert(c);
        c->current_room = newnameroom;
        send_to_client(ec, c, "You have successfully created a room and been redirected to it.\n");
    }
    else if (tok[0] == "/leave")
    {
        if (ntoks == 1)
        {
            if (c->current_room == "general")
            {
                send_to_client(ec, c, "You are in a global room, and it is impossible to leave it.\n");
                return;
            }
            auto it = rooms.find(c->current_room);
            if (it != rooms.end())
            {
                (*it).second.clients_In.erase(c);
            }
            rooms["general"].clients_In.insert(c);
            c->current_room = "general";
            send_to_client(ec, c, "You have left the current room and been successfully redirected to the global room.\n");
            return;
        }
        else
        {
            send_to_client(ec, c, "You have entered extra characters; use /leave.\n");
            return;
        }
    }
    else if (tok[0] == "/users")
    {
        if (ntoks == 1)
        {
            auto it = rooms.find(c->current_room);
            if (it == rooms.end())
            {
                send_to_client(ec, c, "Error: current room not found.\n");
                return;
            }
            size_t sizeroom = (*it).second.clients_In.size();
            std::string users = "Client in room: \n";
            for (Client *cl : (*it).second.clients_In)
            {
                users += "Client " + std::to_string(cl->fd_client) + "\n";
            }
            users += "Total: " + std::to_string(sizeroom);
            send_to_client(ec, c, users);
        }
        else
        {
            send_to_client(ec, c, "You have entered extra characters; use /users.\n");
            return;
        }
    }
    else if (tok[0] == "/rooms")
    {
        if (ntoks == 1)
        {
            std::string countrooms = "Rooms: \n";
            for (auto it = rooms.begin(); it != rooms.end(); it++)
            {
                countrooms += (*it).second.name + '\n';
            }
            countrooms += "Total rooms: " + std::to_string(rooms.size()) + '\n';
            send_to_client(ec, c, countrooms);
            return;
        }
        else
        {
            send_to_client(ec, c, "You have entered extra characters; use /rooms.\n");
            return;
        }
    }
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
        std::string msg_pref_client = "[" + c->username + "]: " + msg + '\n';
        if (!msg.empty() && msg[0] == '/')
        {
            handle_command(c, msg, ec);
            offset += len + 4;
            continue;
        }
        uint32_t new_len_le = msg_pref_client.size();
        uint32_t new_len_be = htonl(new_len_le);
        const char *ptr = reinterpret_cast<const char *>(&new_len_be);
        auto &roomclients = rooms[c->current_room].clients_In;

        for (auto it = roomclients.begin(); it != roomclients.end(); it++)
        {
            if (*it == c || (*it)->alive == false)
            {
                continue;
            }
            if ((*it)->outbuff.size() + static_cast<std::size_t>(new_len_le) + 4 > MAX_INBUF)
            {
                (*it)->alive = false;
                continue;
            }
            (*it)->outbuff.append(ptr, 4);
            (*it)->outbuff.append(msg_pref_client);
            if (!update_events(ec, (*it)))
            {
                (*it)->alive = false;
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

bool set_name(Client *c, int ec)
{
    char buff[1024];
    while (!c->nameenter)
    {
        ssize_t n = recv(c->fd_client, buff, sizeof(buff), 0);
        if (n > 0)
        {
            c->inbuff.append(buff, n);

            if (c->inbuff.size() < 4)
            {
                continue;
            }

            uint32_t len_be = 0;
            memcpy(&len_be, c->inbuff.data(), 4);
            uint32_t len_le = ntohl(len_be);

            if (len_le > 20 || len_le == 0)
            {
                send_to_client(ec, c, "The name must not exceed 20 characters.\n");
                c->inbuff.clear();
                return false;
            }

            if (c->inbuff.size() < 4 + len_le)
            {
                continue;
            }

            std::string nameus(c->inbuff.data() + 4, len_le);
            c->username = nameus;
            c->nameenter = true;
            c->inbuff.erase(0, len_le + 4);

            return true;
        }
        else if (n == 0)
        {
            c->alive = false;
            return false;
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
                return false;
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
    return false;
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

    Room general_room;
    general_room.name = "general";
    rooms["general"] = std::move(general_room);

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
                    rooms["general"].clients_In.insert(c);
                    std::string msg = "[Client " + std::to_string(c->fd_client) + "] connected\n";
                    uint32_t size = htonl(msg.size());
                    for (auto it = rooms["general"].clients_In.begin(); it != rooms["general"].clients_In.end(); it++)
                    {
                        if ((*it) == c)
                        {
                            std::string hello = "Welcome, you’ve connected.\n Enter your name: \n";
                            uint32_t sizeh = htonl(hello.size());
                            const char *ptr = reinterpret_cast<const char *>(&sizeh);
                            c->outbuff.append(ptr, 4);
                            c->outbuff.append(hello);
                            if (!update_events(ec, c))
                            {
                                (*it)->alive = false;
                                break;
                            }
                            continue;
                        }
                        const char *p = reinterpret_cast<const char *>(&size);
                        if ((*it)->outbuff.size() + msg.size() + 4 > MAX_INBUF)
                        {
                            (*it)->alive = false;
                            continue;
                        }
                        (*it)->outbuff.append(p, 4);
                        (*it)->outbuff.append(msg);

                        if (!update_events(ec, (*it)))
                        {
                            (*it)->alive = false;
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
                    if (!c->nameenter)
                    {
                        if (!set_name(c, ec))
                        {
                            continue;
                        }
                        send_to_client(ec, c, "You have successfully entered your name. Now your name in the chat is: |" + c->username + "|\n");
                        if (!c->inbuff.empty())
                        {
                            if(!on_data(c,ec)){
                                c->alive = false;
                                continue;
                            }
                        }
                    }
                    if (c->nameenter)
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
                std::string msg_disconnected_for_all_live = "[" + dead_client->username + "] disconnected\n";
                uint32_t len_be = htonl(msg_disconnected_for_all_live.size());
                const char *ptr = reinterpret_cast<const char *>(&len_be);
                auto it = rooms.find(dead_client->current_room);
                if (it != rooms.end())
                {
                    for (Client *live_client : (*it).second.clients_In)
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
