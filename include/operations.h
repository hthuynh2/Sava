#include "common.h"
#include "UDP.h"
#include "Vertex.h"
#include <dlfcn.h>
#include "App.h"

string create_WR_msg(string file_name);
string create_WT_msg(int rep1, int rep2, int rep3, int version);

string create_WA_msg(int rep1,int  rep2,int rep3, int version, string file_name);
bool write_at_client(string file_name, string sdfs_file_name);
string create_FC_msg(string file_name, int version);
string create_FCR_msg(bool is_accept);
bool check_and_write_file(string file_name, string sdfs_file_name, string dest_ip, string dest_port, int version);

string handle_WR_msg(int socket_fd, string msg, bool need_to_send);
string handle_WA_msg(int socket_fd, string msg, bool need_to_send);
string create_FU_msg(string filename, int rep1, int rep2, int rep3, int version);
string create_DF_msg(string file_name, int version);
void handle_DF_msg(string msg);
bool handle_FC_msg(int socket_fd, string msg);
void handle_FU_msg(string msg);
bool receive_and_store_file(int socket_fd, string file_name);

///
void ls_at_client(string file_name);
string create_LS_msg(string file_name);
string handle_LS_msg(int socket_fd, string msg, bool need_to_send);
///

bool read_at_client(string file_name, string output_file_name);
string create_RFT_msg(string file_name, int version);
string create_RR_msg(string file_name);
string create_RT_msg(bool canRead, int rep1, int rep2, int rep3, int version);
string create_RS_msg(bool canRead);
string handle_RR_msg(int socket_fd, string msg, bool need_to_send);
void handle_RFT_msg(int socket_fd, string msg);
///
void op_handler_thread(int socket_fd);
void op_listening_thread();
void master_handler_thread(int socket_fd);
void master_listening_thread();
void stabilization_handler_thread(int socket_fd);
void stabilization_listening_thread();


///
replicas_t route_file(std::string filename) ;
void handle_update(int& count, std::vector<file_update> & updates, std::mutex & updates_mutex, file_update tup, mutex &count_lock);
void node_fail_handler_at_master(vm_id_t node, int cur_master_id, int cur_master1_id, int cur_master2_id);
void node_fail_handler_at_master1(vm_id_t node);
void node_fail_handler_at_master2(vm_id_t node);
void node_fail_handler(vm_id_t node) ;
string create_FTR_msg(string file_name, int version, int M_x);
string create_MU_msg(int failed_node, int master1_id, int master2_id);
string create_M_msg(int master_id);
string handle_FTR_msg(int socket_fd, string msg, bool need_to_send);
void handle_MU_msg(string msg);
void handle_M_msg(string msg);
/////////



void op_handler_thread(int socket_fd);
void op_listening_thread();
void master_handler_thread(int socket_fd);
void master_listening_thread();
void stabilization_handler_thread(int socket_fd);
void stabilization_listening_thread();

///


bool delete_at_client(string file_name);
string create_DR_msg(string file_name);

string create_DS_msg(bool is_success);
string create_RFR_msg(string file_name);
string create_FTD_msg(string file_name);
string handle_DR_msg(string msg, int socket_fd, bool need_to_send);
void handle_FTD_msg(string msg);

void handle_RFR_msg(string msg);


/////
//NEW MP4

void wait_for_response_thread();
void start_app_at_client(string app_name, string file_name);
void handle_Z_msg(int fd, string msg);
void handle_WU_msg(string input_msg);
void handle_SI_msg(string str);

void worker_listening_thread(Graph_Base* graph_ptr);
void worker_handler_thread(int socket_fd, Graph_Base* graph_ptr);
void worker_listening_from_master_thread(Graph_Base* graph_ptr);
void handle_master_msg_thread(int socket_fd, Graph_Base* graph_ptr);
void start_run_iteration_thread(Graph_Base* graph_ptr);
void run_handle_MS_thread_handler_thread(Graph_Base* graph_ptr, string s_msg);
void start_build_graph_thread(Graph_Base* graph_ptr);
void handle_MI_msg(string str);
void handle_MS_msg(string str);
void Apply_Output_function();
void start_Send_all_messages_to(int dest_worker_id, Graph_Base* graph_ptr);
