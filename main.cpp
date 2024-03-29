//
//  main.cpp
//
//
//  Created by Hieu Huynh on 9/27/17.
//



#include "common.h"
#include "Protocol.h"
#include "UDP.h"
#include "operations.h"
#include "file_management.h"
#include "common1.h"
#include "App.h"
#include "Vertex.h"
#include "master_scheduler.h"
#include "Graph.h"
//#include "PageRank.h"
#include "App_Client.h"

//#include "logging.h"
//
//membership::Logger my_logger;
//membership::Logger::Handle main_log = my_logger.get_handle("Main\t\t\t\t");
//membership::Logger::Handle hb_sender_log = my_logger.get_handle("Heartbeat Sender\t");
//membership::Logger::Handle hb_checker_log = my_logger.get_handle("Heartbeat Checker\t");
//membership::Logger::Handle io_log = my_logger.get_handle("User Input\t\t\t");
//membership::Logger::Handle update_hbs_log = my_logger.get_handle("Heartbeat Targets\t");
//membership::Logger::Handle protocol_log = my_logger.get_handle("Protocol\t\t\t");

unordered_map<int, VM_info> vm_info_map;
set<int> membership_list;
std::mutex membership_list_lock;
set<int> hb_targets;
std::mutex hb_targets_lock;

//No need lock
int my_socket_fd;
VM_info my_vm_info;
void update_hb_targets(bool haveLock);

UDP* my_listener;

string vm_hosts[NUM_VMS] =  {
    "fa17-cs425-g13-01.cs.illinois.edu",
    "fa17-cs425-g13-02.cs.illinois.edu",
    "fa17-cs425-g13-03.cs.illinois.edu",
    "fa17-cs425-g13-04.cs.illinois.edu",
    "fa17-cs425-g13-05.cs.illinois.edu",
    "fa17-cs425-g13-06.cs.illinois.edu",
    "fa17-cs425-g13-07.cs.illinois.edu",
    "fa17-cs425-g13-08.cs.illinois.edu",
    "fa17-cs425-g13-09.cs.illinois.edu",
    "fa17-cs425-g13-10.cs.illinois.edu"
};

void heartbeat_checker_handler();
void get_membership_list(bool is_VM0);
void init_machine();
void msg_handler_thread(string msg);
void heartbeat_sender_handler();
void heartbeat_checker_handler();
void update_hb_targets(bool haveLock);
string int_to_string(int num);
int string_to_int(string str);
void print_membership_list();

bool isJoin;
std::mutex isJoin_lock;
////////


// Membership list ml
using replicas_t = std::set<int>;
using row_num_t = int;
using vm_id_t = int;

mutex master_lock;
int master_id;
int master1_id;
int master2_id;



// File Table Maps
std::map            <row_num_t        , file_row>             file_table;
std::unordered_map    <std::string    , std::set<row_num_t>>    filename_map;
std::unordered_map    <vm_id_t        , std::set<row_num_t>>    replica_map;

// Next File Version
std::unordered_map<std::string    , int>                    next_version_map;



mutex delivered_file_map_lock;
map<string, file_struct> delivered_file_map;
mutex buffer_file_map_lock;
map<string, file_struct> buffer_file_map;       //key = "file_name" + "version"

map<string, long > last_write_map;

mutex waiting_to_handle_fail_id_lock;
int waiting_to_handle_fail_id = -1;
int last_failed_node = -1;

mutex file_table_lock;
mutex next_version_map_lock;

int my_port_offset;
map<string, int> port_map;

//////
//NEW FOR MP4
App_Base* app_ptr;
Master_Scheduler* master_scheduler_ptr;
mutex is_computing_lock;
bool is_computing;
App_Client* app_client_ptr;

int64_t total_receive;
int64_t total_send;


int is_waiting_handle_failer_thread;
mutex is_waiting_handle_failer_thread_lock;

////




//////////
/*This function update the HB targets based on the current membershiplist
 *input:    haveLock: indicate if having lock or not
 *return:   Nothing
 */
void update_hb_targets(bool haveLock){
    if(haveLock == false){
        membership_list_lock.lock();
        hb_targets_lock.lock();
    }

    set<int> new_hb_targets;
    //Get new targets

    auto my_it = membership_list.find(my_vm_info.vm_num);
    int count = 0;

    //Get sucessors
    for(auto it = next(my_it); it != membership_list.end() && count < NUM_TARGETS/2; it++){
        new_hb_targets.insert(*it);
        count ++;
    }

    for(auto it = membership_list.begin(); it != my_it && count < NUM_TARGETS/2; it++){
        new_hb_targets.insert(*it);
        count ++;
    }

    //Get predecessors
    if(count == NUM_TARGETS/2){
        count = 0;
        if(my_it != membership_list.begin()){
            for(auto it = prev(my_it); it != membership_list.begin() && count < NUM_TARGETS/2; it--){
                if(new_hb_targets.find(*it) == new_hb_targets.end()){
                    new_hb_targets.insert(*it);
                    count++;
                }
            }
            if(count < (NUM_TARGETS/2)  && new_hb_targets.find(*membership_list.begin()) == new_hb_targets.end()
               && (*membership_list.begin()) != *my_it){
                count++;
                new_hb_targets.insert(*membership_list.begin());
            }
        }
        for(auto it = prev(membership_list.end()); it != my_it && count < NUM_TARGETS/2; it--){
            if(new_hb_targets.find(*it) == new_hb_targets.end()){
                new_hb_targets.insert(*it);
                count++;
            }
        }
    }
    Protocol p ;
    UDP udp;
    //Old targets is not in new targets, set HB to 0
    for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
        if(new_hb_targets.find(*it) == new_hb_targets.end() && membership_list.find(*it) != membership_list.end()){
            vm_info_map[*it].heartbeat = 0;
            string t_msg = p.create_T_msg();
            udp.send_msg(vm_info_map[*it].ip_addr_str, t_msg);
        }
    }

    //Update targets
    hb_targets.erase(hb_targets.begin(), hb_targets.end());
    for(auto it = new_hb_targets.begin(); it != new_hb_targets.end(); it++){
        hb_targets.insert(*it);
    }

	// Log Updates
//    update_hbs_log << "New targets ";
//    for(auto i : hb_targets) {
//        update_hbs_log << i << " ";
//    }
//    update_hbs_log <<= "";

    if(haveLock == false){
        hb_targets_lock.unlock();
        membership_list_lock.unlock();
    }
    return;
}

/*This function convert the num from 0-99 to a string with 2 char
 *input:    num: number
 *return:   string of that number
 */
string int_to_string(int num){
    string ret("");
    int first_digit = num/10;
    int sec_digit = num%10;
    ret.push_back((char)(first_digit + '0'));
    ret.push_back((char)(sec_digit + '0'));
    return ret;
}

/*This function convert the string of number to int
 *input:    str: string
 *return:    number
 */
int string_to_int(string str){
    int ret = 0;
    for(int i = 0; i < (int)str.size(); i++){
        ret = ret*10 + (str[i] - '0');
    }
    return ret;
}

/*This function print out the membershiplist
 *input:    none
 *return:    none
 */
void print_membership_list(){
    cout <<"Current membership list: \n";
    for(auto it = membership_list.begin(); it != membership_list.end(); it++){
        cout << "id: " << vm_info_map[*it].vm_num << " --- ip address: " << vm_info_map[*it].ip_addr_str
        << " --- time stamp: "<<vm_info_map[*it].time_stamp << "\n";
    }
}


/*This function send request to VM0 to get membershiplist, and set the membership list based on response
 *Input:    bool:
 *Return:   None
 */
void get_membership_list(bool is_vm0){
    UDP local_udp;
    Protocol local_proc;
    string request_msg = local_proc.create_J_msg();
    if(is_vm0 == false){    //If this is not VM0, send request to VM0 until get the response
        cout << "Requesting membership list from VM0...\n";
        while(1){
            local_udp.send_msg(vm_hosts[0], request_msg);
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() == 0) || (i_msg[0] != 'I') ){
                continue;
            }
            local_proc.handle_I_msg(i_msg);
            break;
//            int num_node = string_to_int(i_msg.substr(3,2));        //Initialize the membership list based on VM0 response
//            if((int)i_msg.size() == num_node*16 + 6){
//                local_proc.handle_I_msg(i_msg);
//                break;
//            }
        }
    }
    else{           //If this is VM0, send msg to all other VMs to to check if they are still alive or not
        bool got_msg = false;
        cout << "Checking if there is any VM still alive...\n";
        for(int i = 1; i < NUM_VMS; i++){
            local_udp.send_msg(vm_hosts[i], request_msg);
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() == 0) || (i_msg[0] != 'I') ){
                continue;
            }
            local_proc.handle_I_msg(i_msg);
            got_msg = true;
            break;
            
//            int num_node = string_to_int(i_msg.substr(3,2));        //Set membership list based on the response
//            if((int)i_msg.size() == num_node*16 + 6){
//                got_msg = true;
//                local_proc.handle_I_msg(i_msg);
//                break;
//            }
        }
        if(got_msg == false){
            string i_msg = local_udp.read_msg_non_block(200);
            if((i_msg.size() > 0) && (i_msg[0] == 'I') ){
                local_proc.handle_I_msg(i_msg);
                got_msg = true;
            }
        }
        if(got_msg == false){
            membership_list.insert(0);
            master_id = 0;
            vm_info_map[0] = my_vm_info;
            port_map[my_vm_info.ip_addr_str] = 0;
        }
    }
}

/*This function return ip of this VM
 *Input:    bool:
 *Return:   None
 */
void get_my_ip(){
    struct addrinfo hints, *res, *p;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    char my_addr[512];
    gethostname(my_addr,512);

    if ((status = getaddrinfo(my_addr, NULL, &hints, &res)) != 0) {
        perror("Cannot get my addrinfo\n");
        exit(1);
    }

    for(p = res;p != NULL; p = p->ai_next) {
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
        void * addr = &(ipv4->sin_addr);
        // convert the IP to a string and print it:
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
        break;
    }
    freeaddrinfo(res); // free the linked list

    //Get Bytes from ip address
    unsigned short a, b, c, d;
    sscanf(ipstr, "%hu.%hu.%hu.%hu", &a, &b, &c, &d);
    my_vm_info.ip_addr[0] = (unsigned char) a;
    my_vm_info.ip_addr[1] = (unsigned char) b;
    my_vm_info.ip_addr[2] = (unsigned char) c;
    my_vm_info.ip_addr[3] = (unsigned char) d;

    for (int i = 0 ; i < 4; i++) {
        my_vm_info.ip_addr_str.append(to_string((unsigned int) my_vm_info.ip_addr[i]));
        if(i != 3)
            my_vm_info.ip_addr_str.push_back('.');
    }

    return;
}



string get_ip_vm(string host_name){
    struct addrinfo hints, *res, *p;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    char my_addr[512];
    gethostname(my_addr,512);

    if ((status = getaddrinfo(host_name.c_str(), NULL, &hints, &res)) != 0) {
        perror("Cannot get my addrinfo\n");
        exit(1);
    }

    for(p = res;p != NULL; p = p->ai_next) {
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
        void * addr = &(ipv4->sin_addr);
        // convert the IP to a string and print it:
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
        break;
    }
    freeaddrinfo(res); // free the linked list

    //Get Bytes from ip address
    unsigned short a, b, c, d;
    sscanf(ipstr, "%hu.%hu.%hu.%hu", &a, &b, &c, &d);
    unsigned char temp_ip[4];
    temp_ip[0] = (unsigned char) a;
    temp_ip[1] = (unsigned char) b;
    temp_ip[2] = (unsigned char) c;
    temp_ip[3] = (unsigned char) c;
    string ret("");
    for (int i = 0 ; i < 4; i++) {
        ret.append(to_string((unsigned int) temp_ip[i]));
        if(i != 3)
            ret.push_back('.');
    }
    cout << ret <<"\n";
    return ret;
}


/*This function initilize the vm. It sets my_id, my_id_str, my_logger, my_listener, membership list
 *Input:    None
 *Return:   None
 */
void init_machine(){
    //Init my_id and my_id_str
    get_my_ip();
    bool is_VM0 = false;
    char my_addr[512];
    gethostname(my_addr,512);
    if(strncmp(my_addr, vm_hosts[0].c_str(), vm_hosts[0].size()) == 0){
        is_VM0 = true;
    }


    ///Initialize my_socket_fd
    struct addrinfo hints, *servinfo, *p;
    int rv;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; // set to AF_INET to force IPv4
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE; // use my IP

    if ((rv = getaddrinfo(my_addr,PORT, &hints, &servinfo)) != 0) {
        perror("getaddrinfo: failed \n");
        exit(1);
    }

    // loop through all the results and make a socket
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((my_socket_fd = socket(p->ai_family, p->ai_socktype,
                                   p->ai_protocol)) == -1) {
            perror("server: socket fail");
            continue;
        }
        bind(my_socket_fd, p->ai_addr, p->ai_addrlen);
        break;
    }
    if(p == NULL){
        perror("server: socket fail to bind");
        exit(1);
    }
    freeaddrinfo(servinfo);

    //Initialize UDP listener
    my_listener = new UDP();

    //Get time_stamp
    time_t seconds;
    seconds = time (NULL);
    my_vm_info.time_stamp = to_string(seconds);

    //Get membership_list
    if(is_VM0 == false){
        get_membership_list(false);
    }
    else{
        my_vm_info.vm_num = 0;
        my_vm_info.make_id_str();
        my_vm_info.heartbeat = 0;
        get_membership_list(true);

    }
}

/* This function handles msg based on msg types
 *Input:    msg: message
 *Return:   None
 */
void msg_handler_thread(string msg){
    Protocol local_protocol;
    if(msg[0] == 'H'){
        if(msg.size() != H_MESSAGE_LENGTH)
            return;
        local_protocol.handle_H_msg(msg);
    }
    else if(msg[0] == 'N'){
        if(msg.size() != N_MESSAGE_LENGTH)
            return;
        local_protocol.handle_N_msg(msg, false);

    }
    else if(msg[0] == 'L'){
        if(msg.size() != L_MESSAGE_LENGTH)
            return;
        local_protocol.handle_L_msg(msg, false);
    }
    else if(msg[0] == 'J'){
        if(msg.size() != J_MESSAGE_LENGTH)
            return;
        local_protocol.handle_J_msg(msg);
    } else if(msg[0] == 'G') {
		local_protocol.handle_G_msg(msg, false);
	}
    else if(msg[0] == 'T'){
        local_protocol.handle_T_msg(msg, false);
    }
    else if(msg[0] == 'Q'){
        local_protocol.handle_Q_msg(msg, false);

    }
    //    else if(msg[0] == 'R'){       //Only receive this once when startup. Did it in init_machine
    //        if((msg.size()-2)%12 != 0)
    //            return;
    //        msg_handler.handle_R_msg(msg);
    //    }
}

/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
void listener_thread_handler(){
    vector<std::thread> thread_vector;
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }
        isJoin_lock.unlock();
        string msg = my_listener->read_msg_non_block(500);

        if(msg.size() == 0){
            continue;
        }
        msg_handler_thread(msg);
    }
}

/* This is thread handler to send heartbeats to pre/successors
 *Input:    None
 *Return:   None
 */
void heartbeat_sender_handler(){
    Protocol local_protocol;
    string h_msg = local_protocol.create_H_msg();
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }

        isJoin_lock.unlock();
        UDP local_udp;
        membership_list_lock.lock();
        hb_targets_lock.lock();

        //Send HB to all HB targets
        for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
            if(vm_info_map.find(*it) != vm_info_map.end()){
                local_udp.send_msg(vm_info_map[*it].ip_addr_str, h_msg);
            }
        }

        hb_targets_lock.unlock();
        membership_list_lock.unlock();
        //Sleep for HB_TIME
        std::this_thread::sleep_for(std::chrono::milliseconds(HB_TIME));
    }
}

/* This is thread handler to check heartbeats of pre/successors. If timeout, set that node to DEAD, and send msg
 *Input:    None
 *Return:   None
 */
void heartbeat_checker_handler(){
    while(1){
        isJoin_lock.lock();
        if(isJoin == false){
            isJoin_lock.unlock();
            return;
        }

        isJoin_lock.unlock();
        membership_list_lock.lock();
        hb_targets_lock.lock();
        time_t cur_time;
        cur_time = time (NULL);

        for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
            std::unordered_map<int,VM_info>::iterator dead_vm_it;
            if((dead_vm_it = vm_info_map.find(*it)) != vm_info_map.end()){
                //If current time - last hearbeat > HB_TIMEOUT, mark the VM as dead and send gossip to other VM
                if(cur_time - vm_info_map[*it].heartbeat > HB_TIMEOUT && vm_info_map[*it].heartbeat != 0){
                    VM_info dead_vm = vm_info_map[*it];
                    vm_info_map.erase(dead_vm_it);
                    membership_list.erase(*it);
                    cout << "Failure Detected: VM id: " << dead_vm.vm_num << " --- ip: "<< dead_vm.ip_addr_str << " --- ts: "<<dead_vm.time_stamp
                    << " --- Last HB: " << dead_vm.heartbeat << "--- cur_time:  " << cur_time << "\n";

//                    hb_checker_log << "Failure Detected: VM id: " << dead_vm.vm_num << " --- ip: "<< dead_vm.ip_addr_str << " --- ts: "<<dead_vm.time_stamp
//                    << " --- Last HB: " << dead_vm.heartbeat << "--- cur_time:  " <<= cur_time;
                    print_membership_list();
                    update_hb_targets(true);
                    if(membership_list.size() > 1){
                    Protocol p;
                    p.gossip_msg(p.create_L_msg(dead_vm.vm_num), true);
                    }

                    //MP3:
                    thread node_fail_thread = std::thread(node_fail_handler, dead_vm.vm_num);
                    node_fail_thread.detach();
                }
            }
        }
        hb_targets_lock.unlock();
        membership_list_lock.unlock();
    }
}



std::thread master_thread;
std::thread op_thread;
std::thread stabilization_thread;



void print_file_table(){
    if(master2_id != my_vm_info.vm_num && master1_id != my_vm_info.vm_num && master_id != my_vm_info.vm_num){
        cout << "Not a master. Cannot print file table\n";
        return;
    }
    cout << "Starting to print file table\n";
    file_table_lock.lock();
    cout << "File Table: \n";
    for(auto it = file_table.begin(); it != file_table.end(); it++){
        cout << "row " << it->first << ": ";
        cout << "filename = " << it->second.filename << " " << "rep=" << it->second.replica << " version="<< it->second.version << "\n";
    }
    cout << "Finish printing file table\n";
    file_table_lock.unlock();
}


int main(){
    isJoin = false;

//    string get_ip_vm(string host_name);

//    for(int i = 0 ; i < NUM_VMS; i++){
//        string temp_ip = get_ip_vm(vm_hosts[i]);
//        port_map[temp_ip] = i;
//    }
//    cout << "Hello\n";
//    for(auto it = port_map.begin(); it!= port_map.end(); it++){
//        cout << it->second << " ";
//    }
//    cout << "\n";
    
    //////
    //NEW FOR MP3
    app_ptr =NULL;
    master_scheduler_ptr = NULL;
    is_computing = false;
    
    ////

    std::thread listener_thread;
    std::thread heartbeat_sender_thread;
    std::thread heartbeat_checker_thread;
    cout << "Type JOIN to join the system!\n";
    cout << "Type QUIT to stop the system!\n";
    cout << "Type ML to print membership list\n";
    cout << "Type MyVM to print this VM's information\n";
    cout << "-----------------------------\n";


    master_id = -1;
    master1_id = -1;
    master2_id = -1;
    waiting_to_handle_fail_id = -1;
    last_failed_node = -1;
    
    
    //
    
    
    total_receive = 0;
    total_send = 0;
    is_waiting_handle_failer_thread =0;
    
    //

    ///
//    Graph<PageRankVertex,double> heelo;
//    PageRankVertex v;
//    App_PageRank a;
//    
    ///
    //Main while Loop
    while(1){
        string input;
        getline(cin, input);
        if(strncmp(input.c_str(), "JOIN", 4) == 0){ //Join the system
            isJoin_lock.lock();
            //If user want to join the system
            if(isJoin == true){
                cout << "VM is running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            isJoin = true;
            isJoin_lock.unlock();

            //Initialize the VM
            init_machine();
            cout <<"-----------Successfully Initialize-----------\n";
            cout << "My VM info: id: " << my_vm_info.vm_num << " --- ip: "<< my_vm_info.ip_addr_str << " --- ts: "<<my_vm_info.time_stamp<<"\n";
            print_membership_list();
            //Start all threads
            listener_thread = std::thread(listener_thread_handler);
            heartbeat_sender_thread = std::thread(heartbeat_sender_handler);
            heartbeat_checker_thread = std::thread(heartbeat_checker_handler);


            my_port_offset = my_vm_info.vm_num;

              // MP3
             if(my_vm_info.vm_num == master_id){
                 cout << "I'm Master\n";
             }
              if(my_vm_info.vm_num == master1_id){
                  cout << "I'm Master1\n";
             }
             if(my_vm_info.vm_num == master2_id){
                  cout << "I'm Master2\n";
             }

             master_thread = std::thread(master_listening_thread);
             stabilization_thread = std::thread(stabilization_listening_thread);
             cout << "Start STAB thread Successfully\n";
             op_thread = std::thread(op_listening_thread);
             cout << "Start OP thread Successfully\n";
	   }
        else if(strncmp(input.c_str(), "QUIT", 4) == 0){    //Quit program
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }
            //Set flag to false to stop all threads
            isJoin = false;

            cout <<"Quitting the Program..\n";
            isJoin_lock.unlock();

            Protocol p;
            UDP udp;
            membership_list_lock.lock();
            hb_targets_lock.lock();

            //Send msg to notify other VM before quitting
            string t_msg = p.create_T_msg();
            for(auto it = hb_targets.begin(); it != hb_targets.end(); it++){
                udp.send_msg(vm_info_map[*it].ip_addr_str, t_msg);
            }
            p.gossip_msg(p.create_Q_msg(), true);
            hb_targets_lock.unlock();
            membership_list_lock.unlock();
            break;
        }
        else if(strncmp(input.c_str(), "ML", 2) == 0){            //Print membershiplist
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }

            isJoin_lock.unlock();
            membership_list_lock.lock();
            print_membership_list();
            membership_list_lock.unlock();
        }
        else if(strncmp(input.c_str(), "MyVM", 4) == 0){            //Print VM info
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "VM is NOT running!!!\n";
                isJoin_lock.unlock();
                continue;
            }

            cout << "My VM info: id: " << my_vm_info.vm_num << " --- ip: "<< my_vm_info.ip_addr_str << " --- ts: "<<my_vm_info.time_stamp<<"\n";
            isJoin_lock.unlock();
        }
        ///////////
        else if(strncmp(input.c_str(), "split", 4) == 0){            //Print VM info
            File_Manager temp_f_manager;
            temp_f_manager.split_file("main.cpp", 7, "hieu");
        }
        ///////////
        else{
            isJoin_lock.lock();
            if(isJoin == false){
                cout << "Please JOIN before execute any command....\n";
                isJoin_lock.unlock();
                continue;
            }
            isJoin_lock.unlock();

            istringstream iss(input);
            string word;
            std::vector<string> args;
            while(iss >> word) {
                args.push_back(word);
            }

            cout << "Command: " << input<<"\n";
            cout << "args size = "<< args.size() <<"\n";
            for(int i = 0 ; i < (int)args.size() ; i++){
                cout << args[i] << " ";
            }
            cout <<"\n";

            if(args.empty()){
                cout << "args is empty Undefined command. Please try again.\n";
                continue;
            }
            if(strncmp(args[0].c_str(), "put", 3) == 0){
                if(args.size() != 3){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                cout << "main: " << "put " << args[1] << " " << args[2]<<"\n";
                string local_file_name = args[1];
                string sdfs_file_name = args[2];
                if(local_file_name.size() == 0 || sdfs_file_name.size() == 0){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
				timepnt begin = clk::now();
                write_at_client(args[1], args[2]);
				std::cout << duration_cast_milli(clk::now() - begin).count() << std::endl;
            }
            else if(strncmp(args[0].c_str(), "get", 3) == 0){
                if(args.size() != 3){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                string local_file_name = args[1];
                string sdfs_file_name = args[2];
                if(local_file_name.size() == 0 || sdfs_file_name.size() == 0){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                read_at_client(args[1], args[2]);
            }
            else if(strncmp(args[0].c_str(), "delete", 6) == 0){
                if(args.size() != 2){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                string target_file = args[1];
                if(target_file.size() == 0){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                delete_at_client(target_file);
            }
            else if(strncmp(args[0].c_str(), "ls", 2) == 0){
                if(args.size() != 2){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                string target_file = args[1];
                if(target_file.size() == 0){
                    cout << "Undefined command. Please try again.\n";
                    continue;
                }
                ls_at_client(target_file);
            }
            else if(strncmp(args[0].c_str(), "store", 3) == 0){
                delivered_file_map_lock.lock();
                if(delivered_file_map.size() == 0){
                    cout << "No files is stored at this VM\n";
                }
                else{
                    cout << "Files currently stored in this VM: \n";
                    for(auto it = delivered_file_map.begin(); it != delivered_file_map.end();it++){
                        cout << "......file name = " << it->first<<"\n";
                    }
                }
                delivered_file_map_lock.unlock();
            }
            else if(strncmp(args[0].c_str(), "ms", 2) == 0){
                master_lock.lock();
                cout << "master = " << master_id<<"\n";
                cout << "master1 = " << master1_id<<"\n";
                cout << "master2 = " << master2_id<<"\n";
                master_lock.unlock();
            }
            else if(strncmp(args[0].c_str(), "run", 3) == 0){
                if(args.size() != 2){
                    cout << "Undefined command. Please try again.\n";
                    cout << "run library\n";
                }
                app_client_ptr = new App_Client();
                cout <<"Starting...\n";
                app_client_ptr->start_app_at_client(args[1]);
            }
            else if(strncmp(args[0].c_str(), "ft", 2) == 0){
                cout << "here\n";
                print_file_table();
            }
        }
    }

     cout << "Quit Successfully\n";

    //Wait for all other threads to stop
     listener_thread.join();
     heartbeat_sender_thread.join();
     heartbeat_checker_thread.join();

//    my_logger.write_to_file("vm_log");


    return 0;
}
