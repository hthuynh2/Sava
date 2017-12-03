#ifndef App_Client_h
#define App_Client_h
#include "file_management.h"
#include "common.h"
#include "UDP.h"
#include <unistd.h>
#include <dlfcn.h>
#include "operations.h"
#include "App.h"
#include "master_scheduler.h"

void client_listener_thread(void* local_ptr);
void client_handler_thread(int socket_fd, void* ptr_);


class App_Client{
public:
//    App_Client();
    void start_app_at_client(string lib_name);
    bool client_wait_for_msg(string& msg, int timeout_s);
    void send_cr_msg();
    void client_handle_MZ_msg(int socket_fd, string str);
    bool send_cz_msg_to_master(string dest_ip);
    bool send_cb_msg_to_master(string dest_ip);
    void client_handle_MR_msg(int socket_fd, string str);
    void client_handle_MB_msg(int socket_fd, string str);
    void start_client_listener_thread();
    void client_handle_MF_msg(int socket_fd, string str);
    
private:
    map<string, mutex> msg_lock;
    map<string, int> msg_count;
//    int current_round;
};

extern App_Client* app_client_ptr;

#endif

















