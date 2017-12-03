#include "operations.h"
#include "App.h"
//#include "PageRank.h"
#include "common1.h"
#include "master_scheduler.h"

void wait_for_response_thread(){
    while(1){
        is_computing_lock.lock();
        if(is_computing == false){
            is_computing_lock.unlock();
            return;
        }
        is_computing_lock.unlock();
    }
}


//Handle at Master1 via STAB PORT
void handle_WU_msg(string input_msg){
    string client_id_str = input_msg.substr(1,2);
    int client_id = string_to_int(client_id_str);
    
    string str = input_msg.substr(3);
    
    string app_name;
    stringstream iss(str.c_str());
    iss>>app_name;
    
    string client_ip_;

    if(master_scheduler_ptr == NULL){
        master_scheduler_ptr = new Master_Scheduler(client_id);
    }
    
    string msg = str.substr(app_name.size() +1);
    master_scheduler_ptr->master_handle_WU_msg(msg);
    return;
}


//Handle at Worker via OP PORT
//Handle msg send from master to new worker when one worker failed
void handle_MS_msg(string str){     //"MS1..." "MS0...". Response ..."WS1" or "WS0"
    int temp_num;
    if(str[2] != '0' && str[2] != '1'){
        cout << "Inside handle_MS_msg: Receive Weird msg 1\n";
        return;
    }
    else{
        temp_num = str[2] - '0';
    }
    
    string temp_master_id = str.substr(3,2);
    
    
    //Get library
    if(read_at_client(LIB_FILE_NAME, LIB_FILE_LOCAL) == false){
        //NEED TO DO: need some how to tell the master, o.w it will wait forever
        cout << "Inside handle_MS_msg: CANNOT GET LIB FILE\n";
        return;
    }
    
    //Init App_ptr
    using std::cout;
    using std::cerr;
    // load the triangle library
    void* App_lib_ptr = dlopen(LIB_FILE_LOCAL, RTLD_LAZY);
    if (!App_lib_ptr) {
        cerr << "Cannot load library: " << dlerror() << '\n';
        return;
    }
    
    // reset errors
    dlerror();
    // load the symbols
    create_a* create_app = (create_a*) dlsym(App_lib_ptr, "create_a");
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        cerr << "Cannot load symbol create: " << dlsym_error << '\n';
        return ;
    }
    
    //Load destroy symbol
    destroy_a* destroy_app = (destroy_a*) dlsym(App_lib_ptr, "destroy_a");
    dlsym_error = dlerror();
    if (dlsym_error) {
        cerr << "Cannot load symbol destroy: " << dlsym_error << '\n';
        return ;
    }
    
    //Extract info from msg
    int num_workers = NUM_WORKERS;
    string temp_str = str.substr(5);
    stringstream iss(str.c_str());
    
    map<int, int> worker_map;       //<worker_id, VM id>
    int count = 0;
    string worker_id_str;
    while(num_workers > 0){
        iss >> worker_id_str;
        int worker_id = stoi(worker_id_str);
        worker_map[count] = worker_id;
        count++;
        num_workers--;
    }
    
    int master_vm_num = string_to_int(temp_master_id);

//    master_lock.lock();
//    int master_vm_num = master_id;
//    master_lock.unlock();
//
    //Init graph
    app_ptr = (App_Base*) create_app();
    app_ptr->set_app_info();
    app_ptr->get_graph_ptr()->init_graph(worker_map, master_vm_num);
    
    app_ptr->run_application_thread(temp_num);
}



//Handle at Worker via OP PORT
//Handle msg send from master to new worker when one worker failed
void handle_MI_msg(string str){     //"MI..."
    cout << "handle_MI_msg: " << str <<"\n";
    string temp_master_id = str.substr(2,2);
    cout << "handle_MI_msg: master: " << temp_master_id<<"\n";
    //Get library
    if(read_at_client(LIB_FILE_NAME, LIB_FILE_LOCAL) == false){
        //NEED TO DO: need some how to tell the master, o.w it will wait forever
        cout << "Inside handle_MS_msg: CANNOT GET LIB FILE\n";
        return;
    }
    //Init App_ptr
    using std::cout;
    using std::cerr;
    // load the triangle library
    void* App_lib_ptr = dlopen(LIB_FILE_LOCAL, RTLD_LAZY);
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

    //Extract info from msg
    int num_workers = NUM_WORKERS;
    
    map<int, int> worker_map;       //<worker_id, VM id>
    int count = 0;
    string worker_id_str;
    
    string temp_str = str.substr(4);
    stringstream iss(temp_str.c_str());
    
    while(num_workers > 0){
        iss >> worker_id_str;
        cout << "handle_MI_msg: worker id: " << worker_id_str<< "num_worker = "<<num_workers<<"\n";
        if(worker_id_str.size() != 2){
            continue;
        }
        int worker_id = stoi(worker_id_str);
        worker_map[count] = worker_id;
        count++;
        num_workers--;
    }
    
    int master_vm_num = string_to_int(temp_master_id);

//    master_lock.lock();
//    int master_vm_num = master_id;
//    master_lock.unlock();
    
    //Init graph
    cout << "Start to init app and graph\n";
    app_ptr = (App_Base*) create_app();
    if(app_ptr == NULL){
        cout << "handle_MI_msg: is NULL\n";
    }
    else{
        cout << "handle_MI_msg: is not NULL\n";
    }
    app_ptr->set_app_info();
    cout << "handle_MI_msg: Finish set_app_info. Start to init_graph()\n";
    app_ptr->get_graph_ptr()->init_graph(worker_map, master_vm_num);
    
    cout << "handle_MI_msg: Finish init_graph. Start run_application_thread\n";
    app_ptr->run_application_thread(-1);
    cout << "handle_MI_msg: Finish run_application_thread\n";
}




void worker_listening_thread(Graph_Base* graph_ptr){
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    
    int worker_port_int= stoi(WORKER_PORT);
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
        thread new_thread(worker_handler_thread, new_fd, graph_ptr);
        new_thread.detach();
    }
}



void worker_handler_thread(int socket_fd, Graph_Base* graph_ptr){
    cout << "Inside worker_handler_thread\n";
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
    else if(buf[0] == 'W' && buf[1] == 'B'){
        graph_ptr->handle_WB_msg_from_worker(socket_fd);
//        return;
    }
    else if(buf[0] == 'W' && buf[1] == 'M'){
        graph_ptr->handle_WM_msg(socket_fd);
    }
//    else if(buf[0] == 'B'){
//    }
//    else if(buf[0] == 'M'){
//    }
    else{
        cout << "Inside worker_handler_thread: Receive Undefined msg. Something is WRONG!!!\n";
    }
    close(socket_fd);
    return;
}

void worker_listening_from_master_thread(Graph_Base* graph_ptr){         //Listen msg from master
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    
    int worker_port_int= stoi(WORKER_MASTER_PORT);
    string my_worker_port = to_string(my_port_offset + worker_port_int);
    //    cout << "Stab listening on port "<< my_stab_port<<"\n";
    
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
        thread new_thread(handle_master_msg_thread, new_fd, graph_ptr);
        new_thread.detach();
    }
}


void handle_master_msg_thread(int socket_fd, Graph_Base* graph_ptr){
    cout << "Inside handle_master_msg_thread\n";
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
    else if(buf[0] == 'M' && buf[1] == 'R'){            //Start new round request
        cout << "Inside handle_master_msg_thread: Receive MR\n";
        graph_ptr->handle_MR_msg(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'S'){                //Stop Request
        cout << "Inside handle_master_msg_thread: Receive MS\n";
        graph_ptr->handle_MS_msg(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'G'){                //Local vertice num request
        cout << "Inside handle_master_msg_thread: Receive MG\n";
        string msg = to_string(graph_ptr->get_num_edges_local());
        tcp_send_string(socket_fd, msg);
        
        cout << "TOTAL SEND: " << total_send;
        cout << "TOTAL RECEIVE: " << total_receive;
        return;
    }
    else if(buf[0] == 'M' && buf[1] == 'B'){                    //Build request
        cout << "Inside handle_master_msg_thread: Receive MB\n";
        graph_ptr->handle_MB_msg_from_master(socket_fd, "");
    }
    else if(buf[0] == 'M' && buf[1] == 'V'){                    //Total number of vertices
        cout << "Inside handle_master_msg_thread: Receive MV\n";
        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
            cout << "handle_master_msg_thread:Receive MV: Cannot receive msg\n";
        }
        else{
            string str(buf,numbytes);
            graph_ptr->set_total_vertices(stoi(str));
            cout << "handle_master_msg_thread : Receive MV: total num vertices = " << str<<"\n";
        }
    }
    else if(buf[0] == 'M' && buf[1] == 'O'){                    //Total number of vertices
        cout << "Inside handle_master_msg_thread: Receive MO\n";

        Apply_Output_function();
//        app_ptr->write_to_file();
//        graph_ptr->write_to_file();
        cout << "Inside handle_master_msg_thread: Handle MO DONE\n";
    }
    else{
        cout << "Inside handle_master_msg_thread: Receive Undefined msg. Something is WRONG!!!\n";
    }
    close(socket_fd);
    return;
}


void Apply_Output_function(){
    int64_t total_send = app_ptr->get_graph_ptr()->get_total_send();
    int64_t total_receive = app_ptr->get_graph_ptr()->get_total_receive();
    cout << "Apply_Output_function: total_send = " <<total_send << " ||||| total receive = " << total_receive<<"\n";
    
    
    map<string,string> vertex_value_map;
    app_ptr->get_graph_ptr()->get_all_vertex_value(vertex_value_map);
    app_ptr->Output_function(vertex_value_map);
    app_ptr->write_to_file(OUTPUT_FILE_NAME);
    string sdfs_file_name(OUTPUT_FILE_NAME);
    sdfs_file_name += int_to_string(app_ptr->get_my_worker_id());
    write_at_client(OUTPUT_FILE_NAME, sdfs_file_name);
    return;
}




void start_run_iteration_thread(Graph_Base* graph_ptr){
    graph_ptr->run_iteration();
}


void run_handle_MS_thread_handler_thread(Graph_Base* graph_ptr, string s_msg){
    graph_ptr->handle_MS_thread_handler(s_msg);
}
void start_build_graph_thread(Graph_Base* graph_ptr){
    graph_ptr->build_graph();
}




