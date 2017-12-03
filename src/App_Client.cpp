#include "App_Client.h"

//App_Client::App_Client(){
//    current_round = 0;
//}

void client_upload_file_thread(string file_name);

void App_Client::start_app_at_client(string lib_name){
    //Upload library file
    if(write_at_client(lib_name, LIB_FILE_NAME) == false){
        cout << "start_app_at_client: Cannot upload lib\n";
        return;
    }
    
    using std::cout;
    using std::cerr;
    
    // load the triangle library
    void* App_lib_ptr = dlopen(lib_name.c_str(), RTLD_LAZY);
    if (!App_lib_ptr) {
        cerr << "Cannot load library: " << dlerror() << '\n';
        return ;
    }
    
    // reset errors
    dlerror();
    // load the symbols
    create_a* create_app = (create_a*) dlsym(App_lib_ptr, "create_a_t");
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        cerr << "Cannot load symbol create: " << dlsym_error << '\n';
        return ;
    }
    
    //Load destroy symbol
    destroy_a* destroy_app = (destroy_a*) dlsym(App_lib_ptr, "destroy_a_t");
    dlsym_error = dlerror();
    if (dlsym_error) {
        cerr << "Cannot load symbol destroy: " << dlsym_error << '\n';
        return ;
    }
    
    // create an instance of the class
    App_Base* client_app_ptr = (App_Base*) create_app();
    client_app_ptr->set_app_info();
    
    //Start client_listening thread
    start_client_listener_thread();
    
    //Start thread to upload file
//    thread upload_file_thread(client_upload_file_thread,client_app_ptr->get_file_name());
    client_upload_file_thread(client_app_ptr->get_file_name());

    
    //Send msg to Master via MASTER_PORT
    string cz_msg("CZ");        //Send via MASTER_PORT
    cz_msg += int_to_string(my_vm_info.vm_num);
    
    bool isMaster = false;
    master_lock.lock();
    int cur_master_id = master_id;
    if(my_vm_info.vm_num == master_id){
        isMaster = true;
    }
    master_lock.unlock();
    
    string master_ip;
    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            cout << "Master is currently failed. Please try again later!\n";
            // destroy the class
            destroy_app(client_app_ptr);
            
            // unload the App_lib_ptr library
            dlclose(App_lib_ptr);
//            upload_file_thread.join();
            return ;
        }
        master_ip = vm_info_map[cur_master_id].ip_addr_str;
        membership_list_lock.unlock();
        
        if(send_cz_msg_to_master(master_ip) == false){
            // destroy the class
            destroy_app(client_app_ptr);
            
            // unload the App_lib_ptr library
            dlclose(App_lib_ptr);
//            upload_file_thread.join();
            return;
        }
    }
    else{
        //NEED TO DO: Send msg to itself
        string cz_msg("CZ");
        cz_msg += int_to_string(my_vm_info.vm_num);
        master_handle_cz_msg(cz_msg);
    }
    
    
    
//    upload_file_thread.join();
    

    
    //
    //    if(response_msg == "ZR0"){      //Master send back msg that this App cannot start!
    //        cout << "Cannot run App\n";
    //        return;
    //    }
    //    else{
    //        cout << "Start running App: " << app_name<<"\n";
    //    }
    
    //Send msg to master to tell master start
    if(my_vm_info.ip_addr_str != master_ip){
        if(send_cb_msg_to_master(master_ip) == false){
            return;
        }
    }
    else{
//        NEED TO DO:
        //Send msg to itself
        master_scheduler_ptr->handle_CB_msg(-1, "CB");
    }

    cout << "Successfully send cb_msg to master\n";
    //Wait for job to finish
    msg_lock["MF"].lock();
    msg_count["MF"] = -1;
    msg_lock["MF"].unlock();
    string mf_msg("MF");
    client_wait_for_msg(mf_msg, -1);
    
    
    //Get output files
    for(int i = 0 ; i < NUM_WORKERS; i++){
        string file_name_temp(OUTPUT_FILE_NAME);
        string dest_file_name(LOCAL_OUTPUT_NAME);
        file_name_temp += int_to_string(i);
        dest_file_name += int_to_string(i);
        read_at_client(file_name_temp, dest_file_name);
        cout << "start_app_at_client: Got file with name : "<< file_name_temp<<"\n";
    }
    cout << "start_app_at_client: Finish getting file\n";
    
//    fopen() FINAL_OUTPUT_NAME;
    map<string,string> output_vertex_value_map;
    for(int i = 0 ; i < NUM_WORKERS; i++){
        string dest_file_name(LOCAL_OUTPUT_NAME);
        dest_file_name += int_to_string(i);
        
        cout <<"Reading from file "<< dest_file_name<<"\n";
        std::ifstream infile(dest_file_name.c_str());
        string a, b;
        while (infile >> a >> b){
            if(a.size() == 0){
                continue;
            }
            cout << "a = "<<a << " ||| b = "<< b<<"\n";
            output_vertex_value_map[a] = b;
        }
    }
    
    cout << "start_app_at_client: Finish creating vertex map\n";
    client_app_ptr->Output_function(output_vertex_value_map);
    cout << "start_app_at_client: Finish Output_function\n";

    client_app_ptr->write_to_file(FINAL_OUTPUT_NAME);
    cout << "start_app_at_client: Finish write_to_file\n";

    
//    for(auto it = output_vertex_value_map.size(); it != output_vertex_value_map.end(); it++){
//        cout << it->first << " " <<it->s
//    }
    
    
    
    
    // destroy the class
    destroy_app(client_app_ptr);
    
    // unload the App_lib_ptr library
    dlclose(App_lib_ptr);
}

void client_upload_file_thread(string file_name){
    //Split and upload input files
    File_Manager f_mag;
    f_mag.split_file(file_name, NUM_WORKERS, INPUT_FILE_NAME);
    vector<string> local_files_name;
    for(int i = 0 ; i < NUM_WORKERS; i++){
        string str(INPUT_FILE_NAME);
        str += "a";
        str.push_back((char)('a' + i));
        local_files_name.push_back(str);
    }
    
    cout << "client_upload_file_thread: File name size = " << local_files_name.size() <<"\n";
    for(int i = 0 ; i < (int)local_files_name.size(); i++){
        string str = INPUT_FILE_NAME;
        str += int_to_string(i);
        write_at_client(local_files_name[i], str);
    }
    cout <<"Done!\n";

    return;
}


void App_Client::start_client_listener_thread(){
    thread new_thread(client_listener_thread, (App_Client*)this);
    new_thread.detach();
}



void client_listener_thread(void* local_ptr_){
    App_Client* local_ptr = (App_Client*) local_ptr_;
    
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    
    int worker_port_int= stoi(CLIENT_LISTENER_PORT);
    string my_worker_port = to_string(my_port_offset + worker_port_int);
    
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
        thread new_thread(client_handler_thread, new_fd, local_ptr);
        new_thread.detach();
    }
}


void client_handler_thread(int socket_fd, void* ptr_){
    cout << "Inside client_handler_thread\n";
    App_Client* ptr = (App_Client*) ptr_;
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
    else if(buf[0] == 'M' && buf[1] == 'Z'){
        ptr->client_handle_MZ_msg(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'B'){
        ptr->client_handle_MB_msg(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'R'){
        ptr->client_handle_MR_msg(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'F'){
        ptr->client_handle_MF_msg(socket_fd, "");
    }
    else{
        cout << "Inside client_handler_thread: Receive Undefined msg. Something is WRONG!!!\n";
    }
    close(socket_fd);
    return;
}

void App_Client::client_handle_MF_msg(int socket_fd, string str){
    msg_lock["MF"].lock();
    msg_count["MF"] ++;
    msg_lock["MF"].unlock();
}

void App_Client::client_handle_MZ_msg(int socket_fd, string str){
    msg_lock["CZ"].lock();
    msg_count["CZ"] ++;
    msg_lock["CZ"].unlock();
}


void App_Client::client_handle_MB_msg(int socket_fd, string str){
    msg_lock["CB"].lock();
    msg_count["CB"] ++;
    msg_lock["CB"].unlock();
}


void App_Client::client_handle_MR_msg(int socket_fd, string str){
    string cr_msg("CR");
    char buf[MAX_BUF_LEN];
    int numbytes;
    if(str == "" ){
        if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
            close(socket_fd);
            cout << "client_handle_MB_msg: Receive error!!\n";
            return;
        }
        string round_num_temp(buf,numbytes);
        cr_msg += round_num_temp;
    }
    else{
        cr_msg += str;
    }
    
    msg_lock[cr_msg].lock();
    msg_count[cr_msg] ++;
    msg_lock[cr_msg].unlock();
    return;
}


bool App_Client::send_cb_msg_to_master(string dest_ip){
    cout << "Start Sending CB msg to master\n";
    string cb_msg("CB");
    //Send via master_app_port
    int master_sock_fd = tcp_open_connection(dest_ip, MASTER_APP_PORT);
    if(master_sock_fd == -1){
        cout << "Cannot make connection with master. Please try again later!\n";
        return false;
    }
    msg_lock["CB"].lock();
    msg_count["CB"] = -1;
    msg_lock["CB"].unlock();
    
    int numbytes = tcp_send_string(master_sock_fd, cb_msg);
    if(numbytes != 0){
        cout << "Cannot make connection with master. Please try again later!\n";
        msg_lock["CB"].lock();
        msg_count["CB"] = -1;
        msg_lock["CB"].unlock();
        return false;
    }

//    client_wait_for_msg(cb_msg, -1);
    return true;
}

bool App_Client::send_cz_msg_to_master(string dest_ip){
    string cz_msg("CZ");
    cz_msg += int_to_string(my_vm_info.vm_num);
    cout << "Sending cz_msg_to_master: " <<  cz_msg << "\n";

    //Send via master_app_port
    int master_sock_fd = tcp_open_connection(dest_ip, MASTER_PORT);
    if(master_sock_fd == -1){
        cout << "Cannot make connection with master. Please try again later!\n";
        return false;
    }
    msg_lock["CZ"].lock();
    msg_count["CZ"] = -1;
    msg_lock["CZ"].unlock();
    
    int numbytes = tcp_send_string(master_sock_fd, cz_msg);
    if(numbytes != 0){
        cout << "Cannot make connection with master. Please try again later!\n";
        msg_lock["CZ"].lock();
        msg_count["CZ"] = 0;
        msg_lock["CZ"].unlock();
        return false;
    }
    string cz_str("CZ");
    
    return client_wait_for_msg(cz_str, 5);
}



//NOT USED
void App_Client::send_cr_msg(){
//    current_round++;
    string cr_msg("CR");
//    cr_msg += to_string(current_round);
    
    msg_lock[cr_msg].lock();
    msg_count[cr_msg] = -1;
    msg_lock[cr_msg].unlock();
    
    string dest_ip;
    while(1){
        membership_list_lock.lock();
        if(vm_info_map.find(master_id) != vm_info_map.end()){
            dest_ip = vm_info_map[master_id].ip_addr_str;
            membership_list_lock.unlock();
        }
        else{
            membership_list_lock.unlock();
            cout << "Master is failed. Go to sleep for 2 seconds!!\n";
            sleep(2);
            continue;
        }

        //Send via master_app_port
        int master_sock_fd = tcp_open_connection(dest_ip, MASTER_APP_PORT);
        if(master_sock_fd == -1){
            cout << "Cannot make connection with master. Please try again later!\n";
            continue;
        }
        
        msg_lock[cr_msg].lock();
        if(msg_count[cr_msg] != -1){
            msg_lock[cr_msg].unlock();
            break;
        }
        msg_lock[cr_msg].unlock();
        
        int numbytes = tcp_send_string(master_sock_fd, cr_msg);
        if(numbytes != 0){
            cout << "Cannot make connection with master. Please try again later!\n";
            continue;
        }
        if(client_wait_for_msg(cr_msg, 5) == true){
            break;
        }
    }
    return ;
}

bool App_Client::client_wait_for_msg(string& msg, int timeout_s ){//timeout_in sec
    struct timeval start_tv;
    struct timeval current_tv;
    gettimeofday(&start_tv, NULL);
    cout << "client_wait_for_msg: Start waiting for msg: " << msg<<"\n";
    
    while(1){
        gettimeofday(&current_tv, NULL);
        if(current_tv.tv_sec - start_tv.tv_sec > timeout_s && timeout_s != -1){
            msg_lock[msg].lock();
            msg_count[msg] = 0;
            msg_lock[msg].unlock();
            cout << "client_wait_for_msg: Timeout waiting for msg "<< msg <<"\n";
            return false;
        }
        msg_lock[msg].lock();
        if(msg_count[msg] >= 0){
            msg_lock[msg].unlock();
            cout << "client_wait_for_msg: Received all msg:  "<< msg <<"\n";
            return true;
        }
        msg_lock[msg].unlock();
    }
    return true;
}










