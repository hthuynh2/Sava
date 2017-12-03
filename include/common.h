//
//  common.h
//
//
//  Created by Hieu Huynh on 9/27/17.
//

#ifndef common_h
#define common_h

#define PORT "5392"             //PORT number
#include <dlfcn.h>



#define J_MESSAGE_LENGTH 16     //This is the Length of the J msg
#define H_MESSAGE_LENGTH 4      //This is the Length of the H msg
#define N_MESSAGE_LENGTH 18     //This is the Length of the N msg
#define L_MESSAGE_LENGTH 4      //This is the Length of the L msg

#define GOSSIP_B		3       //This is the parameters B of the gossip algorithm
#define GOSSIP_C		2
#define G_MESSAGE_NRTS	3       //This is the number of rounds to gossiping the msg

#define IP_LEN 4                //LENGTH of IP
#define NUM_TARGETS 4           //NUMBERS OF TARGETS
#define ID_LEN 16               //Length of id of VM

#define VM_AND_TIMESTAMP_SIZE 12
#define ALIVE 1
#define DEAD 0
#define HB_TIME 250     //Send  HB msg every 500ms
#define HB_TIMEOUT 20     // Detect failure if not receive HB after 2 seconds
#define NUM_VMS 10      //NUmber of VMs

#define ERROR_LENGTH		4096
#define MAX_BUF_LEN 1024


///
#define DELETE_RQ_TIMEOUT 5 // in sec
#define STAB_ACK_TIMEOUT 5
#define READ_RQ_TIMEOUT 5
#define WRITE_RQ_TIMEOUT 5
#define WRITE_FC_TIMEOUT 5
#define FILE_TRANS_TIMEOUT 5


#define MASTER_PORT "5000"
#define STABILIZATION_PORT "5010"
#define OP_PORT "5030"
#define NUM_REPLICAS	4  //Might need to change
#define TIMECONFLICT 60

////


#include <stdio.h>
#include <iostream>
#include <vector>
#include <queue>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include "limits.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <algorithm>
#include <queue>
#include "VM_info.h"
#include <set>
#include <unordered_map>
#include <map>

using namespace std;

/////NEW FOR MP4


#define OUT_GOING_MSG_BUF_SIZE 50
#define INPUT_GRAPH_LOCAL "input_graph_local"
#define INPUT_FILE_NAME "input_graph"
#define LIB_FILE_NAME "USER_LIB"
#define LIB_FILE_LOCAL "USER_LIB_LOCAL"

#define NUM_WORKERS 7
#define MASTER_APP_PORT "5050"               //master listen for worker
#define WORKER_MASTER_PORT "5070"           //worker listen for master
#define WORKER_PORT "5090"                  //worker listen to each other
#define CLIENT_LISTENER_PORT "5110"

#define OUTPUT_FILE_NAME "graph_output"
#define FINAL_OUTPUT_NAME "Graph_Final_Output"
#define LOCAL_OUTPUT_NAME "output_from_workers"





///////

extern unordered_map<int, VM_info> vm_info_map;
extern set<int> membership_list;
extern std::mutex membership_list_lock;

extern set<int> hb_targets;
extern std::mutex hb_targets_lock;


//No need lock
extern string time_stamp;
extern string vm_hosts[NUM_VMS];
extern int my_socket_fd;
extern VM_info my_vm_info;
extern void update_hb_targets(bool haveLock);
extern string int_to_string(int num);
extern int string_to_int(string str);
extern void print_membership_list();

extern bool ismeasuring;
extern std::mutex measure_lock;
extern int msg_num;

typedef std::chrono::high_resolution_clock clk;
typedef std::chrono::time_point<clk> timepnt;
typedef std::chrono::milliseconds unit_milliseconds;
typedef std::chrono::microseconds unit_microseconds;
typedef std::chrono::nanoseconds unit_nanoseconds;

#define duration_cast_nano	std::chrono::duration_cast<std::chrono::nanoseconds>
#define duration_cast_micro	std::chrono::duration_cast<std::chrono::microseconds>
#define duration_cast_milli	std::chrono::duration_cast<std::chrono::milliseconds>

using replicas_t = std::set<int>;
using row_num_t = int;
using vm_id_t = int;

// File Updates
// Used in stabilization protocol
struct file_update {
	std::string filename;
	vm_id_t M_y;
	vm_id_t M_x;
	int version;

	file_update(std::string filename, vm_id_t M_y, vm_id_t M_x, int version)
		: filename(filename),
		  M_y(M_y),
		  M_x(M_x),
		  version(version) {
	}
};

// File Table Row
struct file_row {
	row_num_t		row;
	std::string		filename;
	vm_id_t			replica;
	int				version;

    file_row(){}
	file_row(row_num_t row_, std::string filename_, vm_id_t replica_, int version_)
		: row(row_),
		  filename(filename_),
		  replica(replica_),
		  version(version_) {
	}
};
struct file_struct{
    string file_name;       //If in delivered_map. file_name = file name; If in buffer_file_id_map: file_name = file name + version
    int version;
};

extern int master_id;
extern int master1_id;
extern int master2_id;

// File Table Maps
extern std::map	<int, file_row> file_table;
extern std::unordered_map<std::string,std::set<int>>filename_map;
extern std::unordered_map<int, std::set<int>>replica_map;
// Next File Version
extern std::unordered_map<std::string,int>next_version_map;


extern mutex delivered_file_map_lock;
extern map<string, file_struct> delivered_file_map;

extern mutex buffer_file_map_lock;
extern map<string, file_struct> buffer_file_map;       //key = "file_name" + "version"

extern mutex waiting_to_handle_fail_id_lock;
extern int waiting_to_handle_fail_id;
extern int last_failed_node;

extern mutex file_table_lock;
extern mutex next_version_map_lock;

extern mutex master_lock;

extern map<string, long > last_write_map;
////
extern int my_port_offset;
extern map<string, int> port_map;




#endif /* common_h */
