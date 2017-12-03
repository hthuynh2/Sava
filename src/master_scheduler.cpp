#include "master_scheduler.h"


Master_Scheduler::Master_Scheduler(int client_id_){
    super_step = 0;
    membership_list_lock.lock();
    client_id = client_id_;
    int count = 0;
    for(auto it = vm_info_map.begin(); it != vm_info_map.end(); it++){
        if(it->first != client_id){
            worker_vm_map[count] = it->first;
            worker_ip_map[count] = it->second.ip_addr_str;
            vm_worker_map[it->first] = count;
            count++;
            if(count == NUM_WORKERS){
                break;
            }
        }
    }
    
    client_ip = vm_info_map[client_id_].ip_addr_str;
    membership_list_lock.unlock();
    
    done_worker_count = 0;
    is_active = false;
    is_stop = false;
    done_worker_count = 0;
    cur_number = 0;
    waiting_fail_node = -1;
    is_scheduler_runing = false;
    all_vote_to_halt = false;
}

void Master_Scheduler::activate(){
    is_active = true;
}

void Master_Scheduler::handle_node_failure(int node_fail){
    if(vm_worker_map.find(node_fail) == vm_worker_map.end()){
        return;
    }
    
    is_stop_lock.lock();
    if(is_stop == true){                //Might have deadlock. Need to check again
        waiting_fail_node_lock.lock();
        waiting_fail_node = node_fail;
        waiting_fail_node_lock.unlock();
        is_stop_lock.unlock();
        return;
    }
    is_stop_lock.unlock();
    
    
    set_is_stop(true);
    is_handle_failure = true;
    cur_number = (cur_number + 1)%2;
    
    int fail_worker_id = worker_vm_map[node_fail];
    
    worker_vm_map.erase(vm_worker_map[node_fail]);
    worker_ip_map.erase(vm_worker_map[node_fail]);
    vm_worker_map.erase(node_fail);
    
    
    //choose new vm to be new worker
    membership_list_lock.lock();
    for(auto it = vm_info_map.begin(); it != vm_info_map.end(); it++){
        if((it->first != client_id) && (vm_worker_map.find(it->first) != vm_worker_map.end()) && it->first != node_fail){
            worker_vm_map[fail_worker_id] = it->first;
            worker_ip_map[fail_worker_id] = it->second.ip_addr_str;
            vm_worker_map[it->first] = fail_worker_id;
            break;
        }
    }
    membership_list_lock.unlock();
    
    //Update Master_schedule at master1
    master_lock.lock();
    int temp_master1_id = master1_id;
    master_lock.unlock();
    if(my_vm_info.vm_num == temp_master1_id){
        cout << "App_handle_node_failure: handle node failure at master1. Something is wrong!!!\n";
        temp_master1_id = -1;
    }
    
    string master1_ip_str("");
    membership_list_lock.lock();
    if(vm_info_map.find(temp_master1_id) != vm_info_map.end()){
        master1_ip_str = vm_info_map[temp_master1_id].ip_addr_str;
    }
    membership_list_lock.unlock();

    if(master1_ip_str != ""){
        send_worker_vm_map_to_backup_master(master1_ip_str);
    }
    else{
        cout << "App_handle_node_failure: Master1 is dead\n";
    }
    
    //Send Stop msg to all alive workers
    if(send_stop_msg_to_all(node_fail) == false){
        //NEED TO DO: Check if there is any failure need to handle
        cout << "App_handle_node_failure: Cannot send stop msg.\n";
        if(waiting_fail_node != -1){
            int temp = waiting_fail_node;
            waiting_fail_node = -1;
            set_is_stop(false);
            handle_node_failure(temp);
        }
        else{
            set_is_stop(false);
        }
        is_handle_failure = false;
        return;
    }
    
    if(send_MB_msg_to_all() == false){
        //NEED TO DO: Check if there is any failure need to handle
        cout << "App_handle_node_failure: Cannot send BUILD msg.\n";
        if(waiting_fail_node != -1){
            int temp = waiting_fail_node;
            waiting_fail_node = -1;
            set_is_stop(false);
            handle_node_failure(temp);
        }
        else{
            set_is_stop(false);
        }
        is_handle_failure = false;
        return;
    }
    
    //Reset super_step
    super_step = 0;
    
    //Wait for current scheduler thread to stop
    while(1){
        is_scheduler_runing_lock.lock();
        if(is_scheduler_runing == false){
            is_scheduler_runing = true;
            is_scheduler_runing_lock.unlock();
            break;
        }
        is_scheduler_runing_lock.unlock();
    }
    
    

    
    //NEED TO DO: Check if there is any failure need to handle
    if(waiting_fail_node != -1){
        int temp = waiting_fail_node;
        waiting_fail_node = -1;
        set_is_stop(false);
        handle_node_failure(temp);
        is_scheduler_runing_lock.lock();
        is_scheduler_runing = false;
        is_scheduler_runing_lock.unlock();
    }
    else{
        set_is_stop(false);
        //Recompute thread!!
        thread new_thread(scheduler_thread, (void*) this);
        new_thread.detach();
    }
    is_handle_failure = false;
    return;
}

void scheduler_thread(void* ptr_){
    cout << "Inside scheduler_thread\n";
    Master_Scheduler* ptr = (Master_Scheduler*) ptr_;
    ptr->done_worker_count_lock.lock();
    ptr->done_worker_count = 0;
    ptr->done_worker_count_lock.unlock();
    ptr->super_step  = 0;
    
    cout << "scheduler_thread Starting running application.\n";
    while(1){
        ptr->is_stop_lock.lock_shared();
        if(ptr->is_stop == true){
            ptr->is_stop_lock.unlock_shared();
            ptr->is_scheduler_runing_lock.lock();
            ptr->is_scheduler_runing = false;
            ptr->is_scheduler_runing_lock.unlock();
            cout << "scheduler_thread: some node failed. Stop scheduler thread 1\n";
            return;
        }
        ptr->is_stop_lock.unlock_shared();

        ptr->done_worker_count_lock.lock();
        if(ptr->done_worker_count == NUM_WORKERS || ptr->super_step == 0){
            ptr->done_worker_count = 0;
            ptr->done_worker_count_lock.unlock();
            ptr->all_vote_to_halt_lock.lock();
            if(ptr->all_vote_to_halt == true){
                cout << "scheduler_thread: All workers voted to halt\n";
                ptr->all_vote_to_halt = false;
                ptr->all_vote_to_halt_lock.unlock();
                ptr->is_scheduler_runing_lock.lock();
                ptr->is_scheduler_runing = false;
                ptr->is_scheduler_runing_lock.unlock();
                //Tell worker to output into files
                ptr->output_file();
                return;
            }
            else{
                ptr->all_vote_to_halt_lock.unlock();
                ptr->is_stop_lock.lock_shared();
                if(ptr->is_stop == true){
                    ptr->is_stop_lock.unlock_shared();
                    ptr->is_scheduler_runing_lock.lock();
                    ptr->is_scheduler_runing = false;
                    ptr->is_scheduler_runing_lock.unlock();
                    cout << "scheduler_thread: some node failed. Stop scheduler thread 2 \n";
                    return;
                }
                else{   //Send new R msg to workers
                    //Increment current superstep
                    ptr->super_step ++;
                    ptr->is_stop_lock.unlock_shared();
                    cout << "scheduler_thread: Start Send MR msg to all\n";
                    ptr->all_vote_to_halt_lock.lock();
                    ptr->all_vote_to_halt = true;
                    ptr->all_vote_to_halt_lock.unlock();
                    if(ptr->send_MR_msg_to_all(master_scheduler_ptr->super_step) == false) {
                        cout << "scheduler_thread: Cannot send R_msg!!!\n";
                        ptr->is_scheduler_runing_lock.lock();
                        ptr->is_scheduler_runing = false;
                        ptr->is_scheduler_runing_lock.unlock();
                        return;
                    }
                    cout << "scheduler_thread: Finish Send MR msg to all\n";
                }
            }
        }
        ptr->done_worker_count_lock.unlock();
    }
    ptr->is_scheduler_runing_lock.lock();
    ptr->is_scheduler_runing = false;
    ptr->is_scheduler_runing_lock.unlock();
}

bool Master_Scheduler::send_MR_msg_to_all(int cur_step){
    cout << "Inside send_MR_msg_to_all\n";
    done_worker_count_lock.lock();
    done_worker_count = 0;
    done_worker_count_lock.unlock();

    string mr_msg("MR");
    mr_msg += to_string(cur_step);
    cout << "send_MR_msg_to_all: MR msg = " << mr_msg<<"\n";
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ;it++){
        if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT); //Send to WORKER_MASTER_PORT
            if(fd != -1){
                tcp_send_string(fd , mr_msg);
                cout << "send_MR_msg_to_all: sent MR msg to " << worker_ip_map[it->first]<<"\n";
            }
            else{   //Cannot send R_msg
                cout << "send_MR_msg_to_all: Cannot send MR msg\n";
                return false;
            }
        }
        else{
            //Send msg to itself
            cout << "send_MR_msg_to_all: Send msg to itself\n";
            if(app_ptr->get_graph_ptr()){
                app_ptr->get_graph_ptr()->handle_MR_msg(-1, mr_msg);
            }
            else{
                cout << "send_MR_msg_to_all: ERROR: graph ptr = NULL\n";
            }
        }
    }
    return true;
}



bool Master_Scheduler::send_stop_msg_to_all(int fail_worker_id){
    string ms_msg("MS");
    ms_msg += to_string(cur_number);
    ms_msg += int_to_string(my_vm_info.vm_num) + " ";
    int count = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end(); it++){
        if(count == NUM_WORKERS -1){
            ms_msg += int_to_string(it->second);
            break;
        }
        else{
            ms_msg += int_to_string(it->second) + " ";
        }
        count++;
    }
    
    count = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ;it++){
        if(it->second == fail_worker_id){
            int fd = tcp_open_connection(worker_ip_map[it->first], OP_PORT);        //Send to OP_PORT
            if(fd != -1){
                tcp_send_string(fd , ms_msg);
                count++;
            }
            else{
                return false;
            }
        }
        else if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT); //Send to WORKER_MASTER_PORT
            if(fd != -1){
                tcp_send_string(fd , ms_msg);
                count++;
            }
            else{
                return false;
            }
        }
        else{
            //Send msg to itself
            count++;
            //NEED TO DO: NEED to check if it is currentl a worker. IF not, this cause pagefault!
            if(app_ptr != NULL){
                app_ptr->get_graph_ptr()->handle_MS_msg(-1, ms_msg);
            }
//            else{
//                //NEED TO IMPLEMENT
//                thread new_thread(handle_MI_msg, ci_msg);
//                new_thread.detach();
//
//            }
        }
    }

    string temp_ms("MS");
    temp_ms += to_string(cur_number);

    msg_lock[temp_ms].lock();
    msg_count[temp_ms] = - count;
    msg_lock[temp_ms].unlock();
    
    //NEED TO DO: wait until get response to all alive VMs
    while(1){
        msg_lock[temp_ms].lock();
        if(msg_count[temp_ms] == 0){
            msg_lock[temp_ms].unlock();
            return true;
        }
        msg_lock[temp_ms].unlock();
        waiting_fail_node_lock.lock();
        if(waiting_fail_node != -1){
            return false;
            waiting_fail_node_lock.unlock();
        }
        waiting_fail_node_lock.unlock();
    }
}

string Master_Scheduler::create_MI_msg(){
    string msg("MI");
    msg += int_to_string(my_vm_info.vm_num) ;

    int count = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end(); it++){
        if(count == NUM_WORKERS -1){
            msg += int_to_string(it->second);
            break;
        }
        else{
            msg += int_to_string(it->second) + " ";
        }
        count++;
    }
    return msg;
}

void Master_Scheduler::send_worker_vm_map_to_backup_master(string ip_addr){
    string wu_msg = create_WU_msg();
    cout << "send_worker_vm_map_to_backup_master: Sending WU_msg: " << wu_msg <<"\n";
    
    int fd = tcp_open_connection(ip_addr, STABILIZATION_PORT);
    if(fd == -1){
        cout << "send_worker_vm_map_to_backup_master: Cannot make connection\n";
        return;
    }
    tcp_send_string(fd, wu_msg);
    return;
}

string Master_Scheduler::create_WU_msg(){
    string msg("WU");
    msg += int_to_string(client_id);
    msg += " ";
    int count = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end(); it++){
        if(count == NUM_WORKERS -1){
            msg += int_to_string(it->second);
            break;
        }
        else{
            msg += int_to_string(it->second) + " ";
        }
        count++;
    }
    return msg;
}


void Master_Scheduler::master_handle_WU_msg(string input_msg){       //input_msg not include "WU__appname" at the begining;
    cout << "master_handle_WU_msg:  " << input_msg<<"\n";
    int num_workers = NUM_WORKERS;
    stringstream iss(input_msg.c_str());
    
    int count = 0;
    string temp;
    while(num_workers >0){
        iss >>temp;
        worker_vm_map[count] = stoi(temp);
        count++;
        num_workers--;
    }
    membership_list_lock.lock();
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ; it++){
        vm_worker_map[it->second] = it->first;
        worker_ip_map[it->first] = vm_info_map[it->second].ip_addr_str;
    }
    membership_list_lock.unlock();
    return;
}


bool Master_Scheduler::get_is_stop(){
    bool temp ;
    is_stop_lock.lock_shared();
    temp = is_stop;
    is_stop_lock.unlock_shared();
    return temp;
}

void Master_Scheduler::set_is_stop(bool is_stop_){
    is_stop_lock.lock();
    is_stop = is_stop_;
    is_stop_lock.unlock();
    return;
}

void master_listener_thread(void* ptr_){
    Master_Scheduler* ptr = (Master_Scheduler*)ptr_;
    
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    
    int master_app_port= stoi(MASTER_APP_PORT);
    string my_worker_port = to_string(my_port_offset + master_app_port);
    
    //    string port = my_worker_port;
    sockfd = tcp_bind_to_port(my_worker_port);
    
    if (listen(sockfd, 10) == -1) {
        perror("listen");
        exit(1);
    }
    while(1){
        sin_size = sizeof their_addr;
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
        if (new_fd == -1) {
            perror("accept");
            continue;
        }
        thread new_thread(master_msg_handler_thread, new_fd, ptr);
        new_thread.detach();
    }
}


void master_msg_handler_thread(int socket_fd, void* ptr_){
    Master_Scheduler* ptr = (Master_Scheduler*) ptr_;
    
    cout << "Inside master_msg_handler_thread\n";
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    
    if((numbytes = recv(socket_fd, buf, 2, 0)) <= 0 ){
        close(socket_fd);
        cout << "Receive error!!\n";
        return;
    }
    else if(buf[0] == 'W' && buf[1] == 'S'){    //ACK for MS
        cout << "master_msg_handler_thread: received WS msg\n";
        ptr->handle_WS_msg(socket_fd, "");
    }
    else if(buf[0] == 'W' && buf[1] == 'I'){    ////ACK for MI
        cout << "master_msg_handler_thread: received WI msg\n";
        ptr->handle_WI_msg(socket_fd, "");
    }
    else if(buf[0] == 'W' && buf[1] == 'B'){    //ACK for MB
        cout << "master_msg_handler_thread: received WB msg\n";

        ptr->handle_WB_msg(socket_fd, "");
    }
//    else if(buf[0] == 'W' && buf[1] == 'D'){    //ACK for M??
//        ptr->handle_WD_msg(socket_fd, "");
//    }
    else if(buf[0] == 'C' && buf[1] == 'B'){    //Build Request from CLient
        cout << "master_msg_handler_thread: received CB msg\n";
        ptr->handle_CB_msg(socket_fd, "");
    }
    else if(buf[0] == 'W' && buf[1] == 'R'){    //Not use
        cout << "master_msg_handler_thread: received WR msg\n";
        ptr->handle_WR_msg(socket_fd, "");
    }
    else{
        cout << "Inside handle_master_msg_thread: Receive Undefined msg. Something is WRONG!!!\n";
    }
    close(socket_fd);
    return;
}

void Master_Scheduler::handle_WI_msg(int fd, string str){
    cout << "Inside handle_WI_msg\n";
    msg_lock["MI"].lock();
    msg_count["MI"] ++;
    msg_lock["MI"].unlock();
    cout << "Leave handle_WI_msg\n";
    return;
}

void Master_Scheduler::handle_WB_msg(int fd, string str){
    msg_lock["MB"].lock();
    msg_count["MB"] ++;
    cout << "handle_WB_msg: MB msg count = " << msg_count["MB"]<<"\n";
    msg_lock["MB"].unlock();
    return;
}


void Master_Scheduler::handle_CB_msg(int socket_fd, string str){
    cout << "Inside handle_CB_msg\n";
    struct timeval start_time;
    gettimeofday(&start_time, NULL);
    
    if(send_MB_msg_to_all() == false){
        cout << "handle_CB_msg: send_Build_msg_to_all failed\n";
        return;
    }

    is_scheduler_runing_lock.lock();
    if(is_scheduler_runing == true){
        cout << "handle_CB_msg: Some scheduler is running. Something is WRONG\n";
        is_scheduler_runing_lock.unlock();
        return;
    }
    is_scheduler_runing = true;
    is_scheduler_runing_lock.unlock();
    cout << "Start scheduling thread\n";
//    scheduler_thread((Master_Scheduler*) this);
//
//    struct timeval end_time;
//    gettimeofday(&end_time, NULL);
//    cout << "Total Time : " << end_time.tv_sec - start_time.tv_sec <<"s\n";
//
    thread new_thread(scheduler_thread, (Master_Scheduler*)this);
//    new_thread.detach();
    new_thread.join();
    //Send msg to client
    string mf_msg("MF");
    if(socket_fd > 0){
        close(socket_fd);
        int temp_fd =tcp_open_connection(client_ip, CLIENT_LISTENER_PORT);
        if(temp_fd != -1){
            tcp_send_string(temp_fd, mf_msg);
        }
        else{
            cout <<"handle_CB_msg: Cannot make connection to client\n";
        }
    }
    else{
        cout << "handle_CB_msg: NEED TO IMPLEMENT\n";
    }
    cout << "handle_CB_msg: DONE\n";
    return;
}

void Master_Scheduler::handle_WS_msg(int socket_fd, string input_str){
    cout << "handle_WS_msg\n";
    int numbytes;
    char buf[MAX_BUF_LEN];
    string str;
    if(input_str ==  ""){
        if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
            close(socket_fd);
            cout << "handle_WS_msg: Receive error!!\n";
            return;
        }
        str = string(buf,numbytes);
    }
    else{
        str = input_str.substr(2);  //"WS1" or "WS0"
    }
    if(str[0] == '1'){
        if(cur_number == 0){
            cout << "handle_WS_msg: Receive Delay msg. Something is wrong!!\n";
            return;
        }
    }
    else if(str[0] == '0'){
        if(cur_number == 1){
            cout << "handle_WS_msg: Receive Delay msg. Something is wrong!!\n";
            return;
        }
    }
    else{
        cout << "handle_WS_msg: Receive weird msg. Something is wrong!!\n";
        return;
    }
    string temp_msg("MS");
    temp_msg += to_string(cur_number);
    msg_lock[temp_msg].lock();
    msg_count[temp_msg] ++;
    msg_lock[temp_msg].unlock();
}


void Master_Scheduler::handle_WR_msg(int socket_fd, string input_str){
    is_scheduler_runing_lock.lock();
    if(is_scheduler_runing == false){
        is_scheduler_runing_lock.unlock();
        return;
    }
    is_scheduler_runing_lock.unlock();
    
    
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    string str;
    if(input_str == ""){
        if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
            close(socket_fd);
            cout << "Receive error!!\n";
            return;
        }
        str = string(buf, numbytes);
    }
    else{
        str = input_str.substr(2);    //input_str = "WR..."
    }
    cout << "Inside handle_WR_msg msg =" << str<<"\n";

    
    string target_round_str = str.substr(1);
    cout << "handle_WR_msg: Received round = " << target_round_str<<"\n";
    int target_round = stoi(target_round_str);
    if(target_round != super_step){
        cout << "handle_WR_msg: Receive weird msg. Superstep does not match\n";
        return;
    }
    
    bool temp_vote_to_halt = true;
    if(str[0] == '1'){
        //Do nothing
    }
    else if(str[0] == '0'){
        temp_vote_to_halt = false;
    }
    else{
        cout << "handle_WR_msg: Receive weird msg\n";
        return;
    }
    cout <<"handle_WR_msg: HERE\n";
    all_vote_to_halt_lock.lock();
    cout <<"handle_WR_msg: HERE1\n";

    if(temp_vote_to_halt == false){
        cout << "handle_WR_msg: temp_vote_to_halt = FALSE \n";
        all_vote_to_halt = temp_vote_to_halt;
    }
    else{
        cout << "handle_WR_msg: temp_vote_to_halt = True \n";
    }
    all_vote_to_halt_lock.unlock();
    
    done_worker_count_lock.lock();
    done_worker_count++;
    cout << "handle_WR_msg: done_worker_count = " << done_worker_count <<" \n";
    done_worker_count_lock.unlock();
    cout <<"handle_WR_msg : DONE \n";
}



void Master_Scheduler::set_num_vertices(){
    cout << "Inside set_num_vertices\n";
    //Query all workers to get total number of vertices
    string msg("MG");
    
    char buf[MAX_BUF_LEN];
    int numbytes;
    int total = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end(); it++){
        if(it->second != my_vm_info.vm_num){
            int socket_fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT);
            if(socket_fd == -1){
                cout << "get_num_vertices: Cannot make connection\n";
                return;
            }
            tcp_send_string(socket_fd, msg);
            struct timeval timeout_tv;
            timeout_tv.tv_sec = 10;      //in sec
            timeout_tv.tv_usec = 0;
            setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
            if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){      //Might need to change because this wait for so long
                close(socket_fd);
                cout << "get_num_vertices: Receive error\n";
                return;
            }
            string str(buf, numbytes);
            total += stoi(str);
            cout << "set_num_vertices: Get " <<str << " vertices from VM" << it->second<<"\n";
            close(socket_fd);
        }
        else{
            //Send msg to itself
            int temp = app_ptr->get_graph_ptr()->get_num_edges_local();
            app_ptr->get_graph_ptr()->write_graph_to_file();
            cout << "TOTAL SEND BYTES: " << total_send;
            cout << "TOTAL RECEIVE BYTES: " << total_receive;
            total_send= 0;
            total_receive = 0;
            cout << "set_num_vertices: Get " <<temp << " vertices from local graph\n";
            total +=temp;
        }
    }
    
    num_vertices = total;
    cout << "get_num_vertices: Number of vertices = " << total<<"\n";
    
    string v_msg("MV");
    v_msg += to_string(total);
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end(); it++){
        if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT);
            if(fd == -1){
                cout << "get_num_vertices: Cannot make connection\n";
                return;
            }
            tcp_send_string(fd, v_msg);
        }
        else{
            //Send msg to itself
            
            app_ptr->get_graph_ptr()->set_total_vertices(total);
        }
    }
    return;
}

void Master_Scheduler::master_start_init(bool is_listener_running){
    cout << "Inside master_start_init \n";
    if(is_listener_running == false){
        //Start Master listening thread
        cout << "master_start_init: Listener is not running. Start it\n";
        thread master_lis_thr(master_listener_thread, (void*) this);
        master_lis_thr.detach();
    }
    
    if(send_MI_msg_to_all() == false){
        cout << "master_start_init: send_MI_msg_to_all failed\n";
        return;
    }
    else{
        //Send msg to client
        cout << "master_start_init: send_MI_msg_to_all Success\n";
        int temp_fd = tcp_open_connection(client_ip, CLIENT_LISTENER_PORT);
        if(temp_fd == -1){
            cout << "master_start_init: Cannot send result to client\n";
            return;
        }
        string temp_str("MZ");
        tcp_send_string(temp_fd, temp_str);
        cout << "master_start_init: Sent result to client\n";
    }
}



bool Master_Scheduler::send_MI_msg_to_all(){
    string ci_msg = create_MI_msg();
    cout << "Inside send_MI_msg_to_all: " << ci_msg << "\n";
    
    
    
    msg_lock["MI"].lock();
    msg_count["MI"] = 0 - (int) worker_vm_map.size();
    msg_lock["MI"].unlock();
    
    int temp_count = 0;
    
    
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ;it++){
        if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], OP_PORT);    //Send on OP_PORT
            if(fd != -1){
                tcp_send_string(fd , ci_msg);
            }
            else{
                return false;
            }
        }
        else{
            //Send msg to itself
            cout << "send_MI_msg_to_all: asdasdasdasdasdasdasdasdas : " <<temp_count++ <<"\n";
            thread new_thread(handle_MI_msg, ci_msg);
            new_thread.detach();
        }
    }
    
    cout <<"send_MI_msg_to_all: Sent all MI  msg\n";
    
    //NEED TO DO: might need to check is_stop
    string mi_str("MI");
    if(master_wait_for_msg(mi_str, 10) == false){
        cout << "send_MI_msg_to_all: Timeout waiting\n";
        return false;
    }
    else{
        cout << "send_MI_msg_to_all: Timeout waiting\n";
        return true;
    }
}


bool Master_Scheduler::send_MB_msg_to_all(){
    string mb_msg("MB");
    cout << "Start sending MB msg to all workers\n";
    
    msg_lock[mb_msg].lock();
    msg_count[mb_msg] = 0 - (int) worker_vm_map.size();
    msg_lock[mb_msg].unlock();
//    int count = 0;
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ;it++){
        if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT); //Send to WORKER_MASTER_PORT
            if(fd != -1){
                if(tcp_send_string(fd , mb_msg) == -1){
                    cout << "send_MB_msg_to_all: Cannot send\n";
                    return false;
                }
            }
            else{
                cout << "send_MB_msg_to_all: Cannot open connection\n";
                return false;
            }
        }
        else{
            //Send msg to itself
//             count++;
            cout << "send_MB_msg_to_all: Sending MB to itself\n";
            app_ptr->get_graph_ptr()->handle_MB_msg_from_master(-1, "");
//            msg_lock[mb_msg].lock();
//            msg_count[mb_msg] ++;
//            msg_lock[mb_msg].unlock();
        }
    }
    
    while(1){
        msg_lock[mb_msg].lock();
        if(msg_count[mb_msg] >= 0){
            msg_lock[mb_msg].unlock();
            break;
        }
        msg_lock[mb_msg].unlock();
        
        if(is_handle_failure){
            waiting_fail_node_lock.lock();
            if(waiting_fail_node != -1){
                return false;
                waiting_fail_node_lock.unlock();
            }
            waiting_fail_node_lock.unlock();
        }
        else{
            is_stop_lock.lock_shared();
            if(is_stop == true){
                is_stop_lock.unlock_shared();
                return false;
            }
            is_stop_lock.unlock_shared();
        }
    }
    
    cout << "send_MB_msg_to_all: Received all MB. Start calculating num vertices\n";

    set_num_vertices();
    return true;
}

bool Master_Scheduler::master_wait_for_msg(string& msg, int timeout_s ){//timeout_in sec
    cout << "master_wait_for_msg: " << msg <<"\n";
    struct timeval start_tv;
    struct timeval current_tv;
    gettimeofday(&start_tv, NULL);
    
    while(1){
        gettimeofday(&current_tv, NULL);
        if(current_tv.tv_sec - start_tv.tv_sec > timeout_s && timeout_s != -1){
            msg_lock[msg].lock();
            msg_count[msg] = 0;
            msg_lock[msg].unlock();
            cout << "master_wait_for_msg: Timeout waiting for msg "<< msg <<"\n";
            return false;
        }
        msg_lock[msg].lock();
        if(msg_count[msg] >= 0){
            msg_lock[msg].unlock();
            cout << "master_wait_for_msg: Get all msg\n";
            return true;
        }
        msg_lock[msg].unlock();
    }
    cout << "master_wait_for_msg: Get all msg\n";
    return true;
}













///


//NOT USE THIS ONE. USE handle_MI
//Handle at Worker via OP PORT
//void App_handle_I_msg(int socket_fd, string input_str){
//    cout << "Inside App_handle_I_msg\n";
//    int num_workers_str;
//    int numbytes;
//    char buf[MAX_BUF_LEN];
//    string str;
//    if(input_str == ""){
//        if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
//            cout << "Inside App_handle_I_msg: Cannot receive\n";
//            close(socket_fd);   // ???
//            return;
//        }
//        str = string(buf, numbytes);
//        close(socket_fd);   // ???
//    }
//    else{
//        str = input_str.substr(1);
//    }
//
//    string application_name;
//
//    stringstream iss(str.c_str());
//    iss>>application_name;
//
//    int num_workers = NUM_WORKERS;
//
//    string temp_master_id;
//    iss >> temp_master_id;
//    map<int, int> worker_map;       //<worker_id, VM id>
//    int count = 0;
//    string worker_id_str;
//    while(num_workers > 0){
//        iss >> worker_id_str;
//        int worker_id = stoi(worker_id_str);
//        worker_map[count] = worker_id;
//        count++;
//        num_workers--;
//    }
//    master_lock.lock();
//    int master_vm_num = master_id;
//    master_lock.unlock();
//
//
////    app_ptr = (App_Base*) create_app();
////    app_ptr->set_app_info();
////    app_ptr->get_graph_ptr()->init_graph(worker_map, master_vm_num);
////
////
////    start_application(, worker_map, master_vm_num);
//
//
//
//    if(app_ptr == NULL){
//        cout << "Inside App_handle_I_msg: app_ptr = NULL\n";
//        return;
//    }
//    else{
//        //Start new thread to run application!
//        app_ptr->run_application_thread( -1);
//    }
//}
//
//



//Handle at Master via MASTER PORT
void master_handle_cz_msg(string cz_msg){
    string client_id_str = cz_msg.substr(2,2);
    
    string master1_ip_str("");


    
    int client_id = string_to_int(client_id_str);
    cout <<"Inside master_handle_cz_msg: client_id = " << client_id << "\n";
    bool flag = false;
    if(master_scheduler_ptr != NULL){
        cout << "handle_Z_msg: master_scheduler_ptr is Not NULL\n";
        flag = true;
        //        return;   ??? Should return
    }
    else{
        master_scheduler_ptr = new Master_Scheduler(client_id);
    }
    
    master_scheduler_ptr->activate();
    
    //Send backup msg to master 1
    //Get master1_ip
    master_lock.lock();
    int temp_master1_id = master1_id;
    master_lock.unlock();
    
    membership_list_lock.lock();
    if(vm_info_map.find(temp_master1_id) != vm_info_map.end()){
        master1_ip_str = vm_info_map[temp_master1_id].ip_addr_str;
    }
    membership_list_lock.unlock();
    
    
    if(master1_ip_str != ""){
        master_scheduler_ptr->send_worker_vm_map_to_backup_master(master1_ip_str);
    }
    else{
        cout << "App_handle_node_failure: Master1 is dead\n";
    }
    
    master_scheduler_ptr->master_start_init(flag);
    
    //Send response to user
    return;
}

void Master_Scheduler::output_file(){
    send_MO_msg_to_all();
}

bool Master_Scheduler::send_MO_msg_to_all(){
    cout << "Inside send_MO_msg_to_all\n";
    done_worker_count_lock.lock();
    done_worker_count = 0;
    done_worker_count_lock.unlock();
    
    string mr_msg("MO");
    cout << "send_MO_msg_to_all: MO msg = " << mr_msg<<"\n";
    for(auto it = worker_vm_map.begin(); it != worker_vm_map.end() ;it++){
        if(it->second != my_vm_info.vm_num){
            int fd = tcp_open_connection(worker_ip_map[it->first], WORKER_MASTER_PORT); //Send to WORKER_MASTER_PORT
            if(fd != -1){
                tcp_send_string(fd , mr_msg);
                cout << "send_MO_msg_to_all: sent MR msg to " << worker_ip_map[it->first]<<"\n";
            }
            else{   //Cannot send R_msg
                cout << "send_MO_msg_to_all: Cannot send MR msg\n";
                return false;
            }
        }
        else{
            //Send msg to itself
            cout << "send_MO_msg_to_all: Send msg to itself\n";
            cout << "FINAL TOTAL SEND: " << total_send <<"\n";
            cout << "FINAL TOTAL RECEIVE: " << total_receive<<"\n";
            cout << "FINAL: RECV via NET: " << app_ptr->get_graph_ptr()->get_num_msg_receive_via_network() << "\n";
            cout << "FINAL: SEND via NET: " << app_ptr->get_graph_ptr()->get_num_msg_send_via_network() << "\n";
            cout << "FINAL: RECV DIRECTLY: " << app_ptr->get_graph_ptr()->get_num_msg_receive_directly() << "\n";
            cout << "FINAL: SEND DIECTLY " << app_ptr->get_graph_ptr()->get_num_msg_send_directly() << "\n";
            Apply_Output_function();
//            if(app_ptr->get_graph_ptr()){
//                app_ptr->get_graph_ptr()->write_to_file();
//            }
//            else{
//                cout << "send_MO_msg_to_all: ERROR: graph ptr = NULL\n";
//            }
        }
    }
    return true;
}





