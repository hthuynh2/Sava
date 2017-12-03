#ifndef master_scheduler_h
#define master_scheduler_h

#include <shared_mutex>
#include "Vertex.h"
//#include "Graph.h"
#include "UDP.h"
#include "App.h"
#include "common1.h"
#include "operations.h"

void scheduler_thread(void* ptr);
void App_handle_I_msg(int socket_fd, string input_str);
void master_handle_cz_msg(string cz_msg);

void master_listener_thread(void* ptr);

void master_msg_handler_thread(int socket_fd, void* ptr);

class Master_Scheduler{
public:
    
    Master_Scheduler(int client_id_);
    void activate();
    void handle_node_failure(int node_fail);

    bool send_MR_msg_to_all(int cur_step);
    bool send_stop_msg_to_all(int fail_worker_id);

    string create_MI_msg();
    void send_worker_vm_map_to_backup_master(string ip_addr);
    string create_WU_msg();
    void master_handle_WU_msg(string input_msg);
    bool get_is_stop();
    void set_is_stop(bool is_stop_);

    void handle_WI_msg(int fd, string str);
    void handle_WB_msg(int fd, string str);
    void handle_CB_msg(int socket_fd, string str);
    void handle_WS_msg(int socket_fd, string input_str);
    void handle_WR_msg(int socket_fd, string input_str);
    void set_num_vertices();
    void master_start_init(bool is_listener_running);
    bool send_MI_msg_to_all();
    bool send_MB_msg_to_all();
    bool master_wait_for_msg(string& msg, int timeout_s );
    
    bool send_MO_msg_to_all();
    void output_file();
//private:
    int super_step;
    map<int,int > worker_vm_map;    // <worker_id, vm_num>
    map<int,int > vm_worker_map;     //<vm_num, worker_id>
    map<int,string> worker_ip_map;  // <worker_id, ip address>
    bool is_active;
    bool is_stop;                 //Need Read write lock!
    shared_mutex is_stop_lock;
    int done_worker_count;
    mutex done_worker_count_lock;

    int cur_number;     //NEED TO INIT
    
    map<string, int>   msg_count       ; //<msg type, count>
    map<string, mutex> msg_lock   ;
    bool is_handle_failure;
    int waiting_fail_node;
    mutex waiting_fail_node_lock;
    bool is_scheduler_runing;
    mutex is_scheduler_runing_lock;
    bool all_vote_to_halt;
    mutex all_vote_to_halt_lock;
    int client_id ;
    string client_ip;
    int num_vertices;
};

extern Master_Scheduler* master_scheduler_ptr;






#endif



