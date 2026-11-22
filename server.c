#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static volatile sig_atomic_t stopping = 0;
static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t clients_done = PTHREAD_COND_INITIALIZER;
static size_t active_clients = 0;
static size_t total_clients = 0;

typedef struct {
    int fd;
    struct sockaddr_in addr;
} client_info;

static void on_signal(int signum) {
    (void)signum;
    stopping = 1;
}

static void finish_client(int fd) {
    close(fd);
    pthread_mutex_lock(&stats_mutex);
    if (active_clients > 0) active_clients--;
    printf("Отключение: активных клиентов %zu\n", active_clients);
    fflush(stdout);
    pthread_cond_broadcast(&clients_done);
    pthread_mutex_unlock(&stats_mutex);
}

static void *worker(void *argument) {
    client_info *client = argument;
    int fd = client->fd;
    char address[INET_ADDRSTRLEN];
    if (!inet_ntop(AF_INET, &client->addr.sin_addr, address, sizeof(address))) {
        snprintf(address, sizeof(address), "unknown");
    }
    unsigned int port = ntohs(client->addr.sin_port);
    free(client);

    pthread_mutex_lock(&stats_mutex);
    printf("Подключение %s:%u, активных клиентов %zu\n", address, port, active_clients);
    fflush(stdout);
    pthread_mutex_unlock(&stats_mutex);

    char buffer[4096];
    while (!stopping) {
        ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
        if (received == 0) break;
        if (received < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            perror("recv");
            break;
        }
        size_t offset = 0;
        while (offset < (size_t)received && !stopping) {
            ssize_t sent = send(fd, buffer + offset, (size_t)received - offset, MSG_NOSIGNAL);
            if (sent > 0) {
                offset += (size_t)sent;
            } else if (sent < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;
            } else {
                goto done;
            }
        }
    }
done:
    finish_client(fd);
    return NULL;
}

int main(int argc, char **argv) {
    unsigned long port = 9000;
    if (argc > 2) {
        fprintf(stderr, "Использование: %s [порт]\n", argv[0]);
        return 1;
    }
    if (argc == 2) {
        char *end = NULL;
        errno = 0;
        port = strtoul(argv[1], &end, 10);
        if (errno || !end || *end || port == 0 || port > 65535) {
            fprintf(stderr, "Неверный порт\n");
            return 1;
        }
    }

    struct sigaction action = {0};
    action.sa_handler = on_signal;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL) < 0 || sigaction(SIGTERM, &action, NULL) < 0) {
        perror("sigaction");
        return 1;
    }

    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) {
        perror("socket");
        return 1;
    }
    int reuse = 1;
    if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt");
        close(server);
        return 1;
    }
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons((unsigned short)port);
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0 || listen(server, 32) < 0) {
        perror("bind/listen");
        close(server);
        return 1;
    }
    printf("Сервер слушает 127.0.0.1:%lu\n", port);
    fflush(stdout);

    while (!stopping) {
        struct pollfd request = {.fd = server, .events = POLLIN};
        int ready = poll(&request, 1, 200);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (ready == 0) continue;
        if (request.revents & (POLLERR | POLLHUP | POLLNVAL)) break;
        if (!(request.revents & POLLIN) || stopping) continue;

        client_info *client = malloc(sizeof(*client));
        if (!client) {
            perror("malloc");
            continue;
        }
        socklen_t length = sizeof(client->addr);
        client->fd = accept(server, (struct sockaddr *)&client->addr, &length);
        if (client->fd < 0) {
            if (errno != EINTR) perror("accept");
            free(client);
            continue;
        }
        struct timeval timeout = {.tv_sec = 1, .tv_usec = 0};
        setsockopt(client->fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(client->fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        pthread_mutex_lock(&stats_mutex);
        active_clients++;
        total_clients++;
        pthread_mutex_unlock(&stats_mutex);

        pthread_t thread;
        int result = pthread_create(&thread, NULL, worker, client);
        if (result != 0) {
            fprintf(stderr, "pthread_create: %s\n", strerror(result));
            finish_client(client->fd);
            free(client);
            continue;
        }
        result = pthread_detach(thread);
        if (result != 0) fprintf(stderr, "pthread_detach: %s\n", strerror(result));
    }

    stopping = 1;
    close(server);
    pthread_mutex_lock(&stats_mutex);
    while (active_clients > 0) pthread_cond_wait(&clients_done, &stats_mutex);
    printf("Сервер остановлен. Всего подключений: %zu; активных клиентов: %zu\n", total_clients, active_clients);
    pthread_mutex_unlock(&stats_mutex);
    pthread_cond_destroy(&clients_done);
    pthread_mutex_destroy(&stats_mutex);
    return 0;
}
