//#include "Graph.h"
//
//
//template <typename VertexType, typename MessageValue>
//Graph<VertexType,MessageValue>::Graph(bool is_weighted_, map<int,int>& worker_map, int master_vm_num){      //<worker_id, VM id>
//    is_weighted = is_weighted_;
//    membership_list_lock.lock();
//    for(auto it = worker_map.begin(); it != worker_map.end(); it++){
//        if(vm_info_map.find(it->second) == vm_info_map.end()){
//            cout << "Worker is not alive! Something is Wrong!\n";
//            membership_list_lock.unlock();
//            return;
//        }
//        else{
//            worker_ip_map[it->first] = vm_info_map[it->second].ip_addr_str;
//            if(it->second == my_vm_info.vm_num){
//                my_worker_id = it->first;
//            }
//        }
//    }
//    if(vm_info_map.find(master_vm_num) == vm_info_map.end()){
//        cout << "Master is not alive! Something is Wrong!\n";
//        return;
//    }
//    else{
//        master_ip = vm_info_map[master_vm_num].ip_addr_str;
//    }
//
//    membership_list_lock.unlock();
//    num_worker = NUM_WORKERS;
//    send_any_msg = false;
//    super_step = 0;
//    is_running_iteration = false;
//    is_building_graph = false;
//    is_handling_S_msg = false;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::build_graph(){
//    string graph_file(INPUT_GRAPH_LOCAL) ;
//
//    set<string> dest_vertex_set;
//    set<string> source_vertex_set;
//    map<int, vector<string> >  msg_map;           //<worker_id, vector<msg> >
//    string line;
//
//    //Erase everything!!
//    vertex_map.erase(vertex_map.begin(), vertex_map.end());
//    outgoing_msg_map.erase(outgoing_msg_map.begin(), outgoing_msg_map.end());
//    outgoing_msg_count_map.erase(outgoing_msg_count_map.begin(), outgoing_msg_count_map.end());
//    incoming_msg_maps[0].erase(incoming_msg_maps[0].begin(), incoming_msg_maps[0].end());
//    incoming_msg_maps[1].erase(incoming_msg_maps[1].begin(), incoming_msg_maps[1].end());
//
//    //Make connection to all other workers
//    map<int, int> fd_map;                   //<worker_id, socket>
//    for(auto it =  worker_ip_map.begin(); it != worker_ip_map.end(); it++){
//        int temp_fd = tcp_open_connection(it->second, WORKER_PORT);
//        if(temp_fd == -1){
//            cout << "build_graph: Cannot make connection\n";
//            return;
//        }
//        fd_map[it->first] = temp_fd;
//    }
//
//    ifstream file_stream(graph_file.c_str());
//    if (file_stream.is_open()){
//        while(getline(file_stream,line)){
//            if(line[0] <= '9' && line[0] >= '0' && line.size() > 0)
//                if(handle_input_line(dest_vertex_set, source_vertex_set, line) == false){
//                    cout << "handle_input_line: Some error happen\n";
//                    return;
//                }
////            cout << line << '\n';
//        }
//        file_stream.close();
//    }
//    else{
//        cout << "build_graph: Unable to open file";
//    }
//
//    for(auto it = msg_map.begin(); it != msg_map.end();it++){
//        if(it->second.size() >0){
//            if(it->first == my_worker_id){
//                cout << "build graph: send msg to itself. Something is wrong\n";
//            }
//            else{
//                send_all_msg_in_build_buffer(fd_map[it->first], it->second);
//                msg_map[it->first].erase(msg_map[it->first].begin(), msg_map[it->first].end());
//            }
//        }
//    }
//
//
//
//    //Send dest_vertex
//    for(auto it = dest_vertex_set.begin(); it != dest_vertex_set.end() ;it++){
//        if(source_vertex_set.find(*it) == source_vertex_set.end()){
//            int dest_worker_id = str_hash(*it) % num_worker;
//            if(dest_worker_id != my_worker_id){
//                string msg("");
//                msg += *it + "\n";
//                msg_map[dest_worker_id].push_back(msg);
//                if(msg_map[dest_worker_id].size() >= 99 ){
//                    send_all_msg_in_build_buffer(fd_map[dest_worker_id], msg_map[dest_worker_id]);
//                    msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
//                }
//            }
//            else{
//                vertex_map[*it].set_vertex_id(*it);
//                vertex_map[*it].setGraphPtr((Graph_Base*) this);
//            }
//        }
//    }
//
//
//    for(auto it = msg_map.begin(); it != msg_map.end();it++){
//        if(it->second.size() >0){
//            if(it->first == my_worker_id){
//                cout << "build graph: send msg to itself. Something is wrong\n";
//            }
//            else{
//                send_all_msg_in_build_buffer(fd_map[it->first], it->second);
//                msg_map[it->first].erase(msg_map[it->first].begin(), msg_map[it->first].end());
//            }
//        }
//    }
//
//
//    string d_msg("D");
//    d_msg += int_to_string(my_worker_id);
//
//    if(master_ip != my_vm_info.ip_addr_str){
//        int master_fd_temp = tcp_open_connection(master_ip, MASTER_APP_PORT);
//        if(master_fd_temp == -1){           //Maybe should wait for new master
//            cout << "build graph: Cannot make connection to master\n";
//            return;
//        }
//        tcp_send_string(master_fd_temp, d_msg);
//    }
//    else{
//        master_scheduler_ptr->handle_D_msg(-1, d_msg);
//    }
//}
//
//
//template <typename VertexType, typename MessageValue>
//bool Graph<VertexType,MessageValue>::handle_input_line(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line,
//                       map<int, vector<string> >&  msg_map)
//{
//    if(line[0] > '9' || line[0] < '0'){
//        cout << "handle_input_line: " <<  line;
//        return false;
//    }
//    stringstream iss(line);
//    string source, end, weight;
//
//    if(is_weighted == true){
//        iss >> source;
//        iss >> end;
//        iss >> weight;
//
//        source_vertex_set.insert(source);
//        if(source_vertex_set.find(end) == source_vertex_set.end()){
//            dest_vertex_set.insert(end);
//        }
//
//        int dest_worker_id = str_hash(source) % num_worker;
//
//        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
//            if(vertex_map.find(source) == vertex_map.end() ){
//                vertex_map[source].set_vertex_id(source);
//                vertex_map[source].add_edge(end, weight);
//                vertex_map[source].setGraphPtr((Graph_Base*) this);
//            }
//        }
//        else{
//            string msg("");
//            msg  += source + " " + end + " " + weight + "\n";
//            msg_map[dest_worker_id].push_back(msg);
//            if(msg_map[dest_worker_id].size() >= 99){
//                //Send all msg
//                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id]) == false){
//                    cout << "handle_input_line: fail to send all msg\n";
//                    return false;
//                }
//                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
//            }
//        }
//    }
//    else{
//        iss >> source;
//        iss >> end;
//
//        source_vertex_set.insert(source);
//        if(source_vertex_set.find(end) == source_vertex_set.end()){
//            dest_vertex_set.insert(end);
//        }
//
//        int dest_worker_id = str_hash(source) % num_worker;
//
//        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
//            if(vertex_map.find(source) == vertex_map.end() ){
//                vertex_map[source].set_vertex_id(source);
//                vertex_map[source].add_edge(end);    //Might be wrong!!!
//                vertex_map[source].setGraphPtr((Graph_Base*) this);
//            }
//        }
//        else{
//            string msg("");
//            msg  += source + " " + end  + "\n";
//            msg_map[dest_worker_id].push_back(msg);
//            if(msg_map[dest_worker_id].size() >= 99){
//                //Send all msg
//                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id]) == false){
//                    cout << "handle_input_line: fail to send all msg\n";
//                    return false;
//                }
//                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
//            }
//        }
//    }
//    return true;
//}
//
//template <typename VertexType, typename MessageValue>
//bool Graph<VertexType,MessageValue>::send_all_msg_in_build_buffer(int dest_worker_id, vector<int>& msg_v){
//    if(msg_v.size() > 99){
//        cout << "send_all_msg_in_build_buffer: msg_v.size() > 99. Something is WRONG\n";
//        return false;
//    }
//
//    string msg("B");
//    msg += int_to_string(msg_v.size());
//
//    for(int i = 0; i < (int) msg_v.size(); i++){
//        msg += msg_v[i];
//    }
//    int temp_fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
//    if(temp_fd == -1){
//        cout << "send_all_msg_in_build_buffer:Cannot open connection. Something is WRONG\n";
//        return false;
//    }
//    if(tcp_send_string(temp_fd, msg) == -1){
//        cout << "send_all_msg_in_build_buffer:Cannot send. Something is WRONG\n";
//        return false;
//    }
//    return true;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::run_iteration(){
//    if(super_step == 1){
//        inactive_vertices.erase(inactive_vertices.begin(), inactive_vertices.end());
//        for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
//            active_vertices.insert(it->first);
//            inactive_vertices.erase(it->first);
//        }
//    }
//
//    int map_idx = super_step%2;
//    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
//        if(incoming_msg_maps[map_idx].find(it->first) != incoming_msg_maps[map_idx].end(it->first)
//           || active_vertices.find(it->first) != active_vertices.end() ){
//            it->second.compute(incoming_msg_maps[map_idx][it->first]);
//        }
//    }
//    incoming_msg_maps[map_idx].erase(incoming_msg_maps[map_idx].begin(), incoming_msg_maps[map_idx].end());
//    if(send_any_msg == false && active_vertices.empty()){
//        send_end_iteration_msg_to_master(true);
//    }
//    else{
//        send_end_iteration_msg_to_master(false);
//    }
//    is_running_iteration_lock.lock();
//    is_running_iteration = false;
//    is_running_iteration_lock.unlock();
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::send_end_iteration_msg_to_master(bool isDone){
//    string msg("");
//    if(isDone){
//        msg += "F1";
//        msg += to_string(super_step);
//    }
//    else{
//        msg += "F0";
//        msg += to_string(super_step);
//    }
//
//    if(my_vm_info.ip_addr_str !=master_ip ){
//        int master_fd = tcp_open_connection(master_ip, MASTER_APP_PORT);
//        if(master_fd == -1){
//            return;
//        }
//        tcp_send_string(master_fd, msg);
//    }
//    else{
//        master_scheduler_ptr->handle_F_msg(-1,msg);
//    }
//    return;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::set_master_ip(string master_ip_){
//    master_ip = master_ip_;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::set_worker_ip_map(map<int,int>& worker_map){
//    membership_list_lock.lock();
//    for(auto it = worker_map.begin(); it != worker_map.end(); it++){
//        if(vm_info_map.find(it->first) == vm_info_map.end()){
//            cout << "Worker is not alive! Something is Wrong!\n";
//        }
//        else{
//            worker_ip_map[it->first] = vm_info_map[it->first].ip_addr_str;
//        }
//    }
//    membership_list_lock.unlock();
//}
//
//template <typename VertexType, typename MessageValue>
//int Graph<VertexType,MessageValue>::get_num_worker(){
//    return (int) worker_ip_map.size();
//}
//
//template <typename VertexType, typename MessageValue>
//int Graph<VertexType,MessageValue>::get_super_step(){
//    return super_step;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::set_super_step(int super_step_){
//    super_step = super_step_;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::SendMessageTo(const string& dest_vertex, void* message_ptr) {
//    if(message_ptr == NULL)
//        return;
//    MessageValue* msg_ptr = (MessageValue*) message_ptr;
//    MessageValue msg = * msg_ptr;
//
//    send_any_msg = true;
//    int dest_worker_id = str_hash(dest_vertex) % num_worker;
//    string dest_worker_ip = worker_ip_map[dest_worker_id];
//
//    if(dest_worker_id == my_worker_id){
//        //MOVE MSG TO MSG QUEUE
//        int map_idx = (super_step + 1) %2;
//        incoming_msg_maps[map_idx][dest_vertex].push_back(msg);
//        return;
//    }
//
//
////    map<string, vector<MessageValue> > outgoing_msg_map   ;     //<dest_vertex_id, out-msg-buffer>
//    outgoing_msg_map[dest_worker_id][dest_vertex].push_back(msg);
//    outgoing_msg_count_map[dest_worker_id]++;
//    if(outgoing_msg_count_map[dest_worker_id] == OUT_GOING_MSG_BUF_SIZE){
//        vector<string> vertex_v;
//        vector<MessageValue> msg_value_v;
//        for(auto it = outgoing_msg_map[dest_worker_id].begin(); it != outgoing_msg_map[dest_worker_id].end(); it++){
//            for(auto it1 = it->second.begin(); it1 != it->second.end(); it1++){
//                vertex_v.push_back(it->first);
//                msg_value_v.push_back(*it1);
//            }
//        }
//        string msg_m = create_M_msg(vertex_v, msg_value_v);
//        int temp_socket_fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
//
//        if(temp_socket_fd == -1){
//            cout << "SendMessageTo: Cannot send msg. Something is WRONG!!!\n";
//        }
//        else{
//            tcp_send_string(temp_socket_fd, msg_m);
//        }
//        outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end());
//        outgoing_msg_count_map[dest_worker_id] = 0;
//    }
//}
//
//template <typename VertexType, typename MessageValue>
//string Graph<VertexType,MessageValue>::create_M_msg(vector<string>& vertex_v, vector<MessageValue>& msg_value_v){
//    string msg("M");
//    if(vertex_v.size() > 99 || vertex_v.size() == 0){
//        cout << "create_M_msg: ERROR: vertex_v has size > 99 or empty!!!!. Size = " << vertex_v.size() << "\n";
//        return msg;
//    }
//    msg += to_string(vertex_v.size());
//    for(int i = 0 ; i < (int) vertex_v.size() ;i++){
//        msg += vertex_v[i] ;
//        if(i != vertex_v.size() -1){
//            msg += " ";
//        }
//        else{
//            msg += "\n";
//        }
//    }
//
//    for(int i = 0; i < (int) msg_value_v.size(); i++){
//        string msg_val_str = MessageValue_to_String(msg_value_v[i]);
//        msg += msg_val_str;
//    }
//    //No '\n' at the end!!!!
//    return msg;
//}
//
//template <typename VertexType, typename MessageValue>
//string Graph<VertexType,MessageValue>::MessageValue_to_String(MessageValue& val){
//    char buf[sizeof(MessageValue)];
//    memcpy((void*)(buf), (void*)(&val), sizeof(MessageValue));
//    string str(buf, sizeof(MessageValue));
//    return str;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::String_to_MessageValue(string& str, MessageValue& val){
//    if(str.size() != sizeof(MessageValue)){
//        cout << "String_to_MessageValue: Str has size " << str.size() << "\n";
//        return;
//    }
//    memcpy((void*)(&val), (void*)(str.c_str()), sizeof(MessageValue));
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::worker_handler_thread(int socket_fd){
//    cout << "Inside worker_handler_thread\n";
//    int numbytes = 0;
//    char buf[MAX_BUF_LEN];
//    struct timeval timeout_tv;
//    timeout_tv.tv_sec = 10;      //in sec
//    timeout_tv.tv_usec = 0;
//    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
//
//    if((numbytes = recv(socket_fd, buf, 1, 0)) <= 0 ){
//        close(socket_fd);
//        cout << "Receive error!!\n";
//        return;
//    }
//    else if(buf[0] == 'B'){
//        handle_B_msg_from_worker(socket_fd);
//    }
//    else if(buf[0] == 'M'){
//        handle_M_msg(socket_fd);
//    }
//    else{
//        cout << "Inside worker_handler_thread: Receive Undefined msg. Something is WRONG!!!\n";
//    }
//    close(socket_fd);
//    return;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_B_msg_from_worker(int socket_fd){
//    cout << "Inside handle_B_msg\n";
//    int num_vertices = 0;
//    int numbytes;
//    char buf[MAX_BUF_LEN*2];
//
//    if((numbytes = recv(socket_fd, buf, 2, 0)) < 2 ){
//        cout << "Inside handle_B_msg: Receive error!!\n";
//        return;
//    }
//    if(buf[0] > '9' || buf[0] < '0' ||buf[1] > '9' || buf[1] < '0' ){
//        cout << "Inside handle_B_msg: Error with number vertices\n";
//        return;
//    }
//
//    num_vertices = (buf[0] - '0')*10 + (buf[1] - '9');
//    while(num_vertices > 0){
//        if((numbytes = recv(socket_fd, buf, 1500, 0)) <= 0 ){           //Might be wrong!?!
//            cout << "Inside handle_B_msg: Receive error 1!!\n";
//            return;
//        }
//        if(buf[numbytes-1] != '\n'){
//            char c[1];
//            int temp_numbytes;
//            while(1){
//                if((numbytes = recv(socket_fd, c, 1, 0)) <= 0){
//                    cout << "Inside handle_B_msg: Receive error 2!!\n";
//                    return;
//                }
//                else{
//                    if(numbytes >= 2*MAX_BUF_LEN){
//                        cout << "Inside handle_B_msg: Buffer Overflow !!\n";
//                        return;
//                    }
//                    buf[numbytes] = c[0];
//                    numbytes++;
//                    if(c[0] == '\n'){
//                        break;
//                    }
//                }
//            }
//        }
//
//        string str(buf, numbytes);
//
//        std::stringstream ss(str.c_str());
//        std::string temp_str;
//        while(getline(ss, temp_str, '\n')){
//            stringstream iss(temp_str.c_str());
//            string source , end, weight;
//
//            iss >> source;
//            iss >> end;
//            iss >> weight;
//            vertex_map[source].set_vertex_id(source);
//            if(end.size() == 0 && weight.size() == 0){
//                num_vertices --;
//                continue;
//            }
//            if(is_weighted == false){
//                vertex_map[source].add_edge(end);
//                vertex_map[source].setGraphPtr((Graph_Base*) this);
//                num_vertices --;
//            }
//            else{
//                if(weight.size() == 0){
//                    cout << "Inside handle_B_msg: Weight.size() == 0. Something is WRONG!!!!\n";
//                    return;
//                }
//                vertex_map[source].add_edge(end, weight);
//                vertex_map[source].setGraphPtr((Graph_Base*) this);
//                num_vertices --;
//            }
//        }
//    }
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_M_msg(int socket_fd){
//    cout << "Inside handle_M_msg\n";
//    int num_vertices = 0;
//    int numbytes;
//    char buf[2*MAX_BUF_LEN];
//
//    if((numbytes = recv(socket_fd, buf, 2, 0)) < 2 ){
//        cout << "Inside handle_M_msg: Receive error 1 !!\n";
//        return;
//    }
//    if(buf[0] > '9' || buf[0] < '0' ||buf[1] > '9' || buf[1] < '0' ){
//        cout << "Inside handle_M_msg: Error with number vertices\n";
//        return;
//    }
//
//    num_vertices = (buf[0] - '0')*10 + (buf[1] - '9');
//
//    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
//        cout << "Inside handle_M_msg: Receive error 2 !!\n";
//        return;
//    }
//
//    string str(buf, numbytes);
//    std::stringstream ss(str.c_str());
//    std::string temp_str;
//    if(!getline(ss, temp_str, '\n')){
//        cout << "Inside handle_M_msg: Getline: Some thing is wrong!!???\n";
//        return;
//    }
//    int remain = numbytes - (temp_str.size() -1);
//    stringstream iss(temp_str.c_str());
//    string temp_vertex_id;
//    vector<string> vertex_v;
//    while(iss >> temp_vertex_id){
//        vertex_v.push_back(temp_vertex_id);
//    }
//    if(num_vertices != (int)vertex_v.size() ){
//        cout << "Inside handle_M_msg: Does not receive enough vertex. Something is wrong!!???\n";
//        return;
//    }
//
//
//    ///NEED TO DO: Some calculation might be wrong. Need to check again!!
//    int offset = temp_str.size()+1;
//    int total = remain;
//    while(total/sizeof(MessageValue) < num_vertices){
//        if((numbytes = recv(socket_fd, (void*)(buf+numbytes), 2*MAX_BUF_LEN-numbytes, 0)) <= 0 ){
//            cout << "Inside handle_M_msg: Receive error 3!!\n";
//            return;
//        }
//        total += numbytes;
//    }
//
//    vector<MessageValue> value_v;
//
//    while(value_v.size() < num_vertices){
//        string value_str((char*) (buf + offset), sizeof(MessageValue));
//        offset += sizeof(MessageValue);
//        MessageValue temp_val;
//        String_to_MessageValue(value_str, temp_val);
//        value_v.push_back(temp_val);
//    }
//
//    int map_idx = (super_step + 1) %2;
//
//    for(int i = 0 ; i < (int)value_v.size(); i++){
//        incoming_msg_maps[map_idx][vertex_v[i]].push_back(value_v[i]);
//    }
//
//    return;
//}
//
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::worker_listening_thread(){
//    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
//    struct addrinfo hints, *servinfo, *p;
//    struct sockaddr_storage their_addr; // connector's address information
//    socklen_t sin_size;
//
//    int worker_port_int= stoi(WORKER_PORT);
//    string my_worker_port = to_string(my_port_offset + worker_port_int);
//
//    //    string port = my_worker_port;
//    sockfd = tcp_bind_to_port(my_worker_port);
//
//    if (listen(sockfd, 10) == -1) {
//        perror("listen");
//        exit(1);
//    }
//    while(1){
//        sin_size = sizeof their_addr;
//        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
//        if (new_fd == -1) {
//            perror("accept");
//            continue;
//        }
//        thread new_thread(worker_handler_thread, new_fd);
//        new_thread.detach();
//    }
//}
//
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::worker_listening_from_master_thread(){         //Listen msg from master
//    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
//    struct addrinfo hints, *servinfo, *p;
//    struct sockaddr_storage their_addr; // connector's address information
//    socklen_t sin_size;
//
//    int worker_port_int= stoi(WORKER_MASTER_PORT);
//    string my_worker_port = to_string(my_port_offset + worker_port_int);
////    cout << "Stab listening on port "<< my_stab_port<<"\n";
//
//    //    string port = my_worker_port;
//    sockfd = tcp_bind_to_port(my_worker_port);
//
//    if (listen(sockfd, 10) == -1) {
//        perror("listen");
//        exit(1);
//    }
//    while(1){
//        sin_size = sizeof their_addr;
//        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
//        if (new_fd == -1) {
//            perror("accept");
//            continue;
//        }
//        thread new_thread(handle_master_msg_thread, new_fd);
//        new_thread.detach();
//    }
//}
//
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_master_msg_thread(int socket_fd){
//    cout << "Inside handle_master_msg_thread\n";
//    int numbytes = 0;
//    char buf[MAX_BUF_LEN];
//    struct timeval timeout_tv;
//    timeout_tv.tv_sec = 10;      //in sec
//    timeout_tv.tv_usec = 0;
//    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
//
//    if((numbytes = recv(socket_fd, buf, 1, 0)) <= 0 ){
//        close(socket_fd);
//        cout << "Receive error!!\n";
//        return;
//    }
//    else if(buf[0] == 'R'){
//        handle_R_msg(socket_fd, "");
//    }
//    else if(buf[0] == 'B'){
//        handle_B_msg_from_master(socket_fd, "");
//    }
//    else if(buf[0] == 'S'){
//        //Stop all threads that is running!!!!!
//        handle_S_msg(socket_fd, "");
//    }
//    else if(buf[0] == 'G'){
//        string msg = to_string(get_num_vertices_local());
//        tcp_send_string(socket_fd, msg);
//        return;
//    }
//    else if(buf[0] == 'V'){
//        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
//            cout << "handle_master_msg_thread: Cannot receive msg\n";
//        }
//        else{
//            string str(buf,numbytes);
//            set_total_vertices(stoi(str));
//
//        }
//    }
//    else{
//        cout << "Inside handle_master_msg_thread: Receive Undefined msg. Something is WRONG!!!\n";
//    }
//    close(socket_fd);
//    return;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_S_thread_handler(string str){   //"1I...." or "0I..."
//
//    //Note: Might need to also stop receiving all msg from other workers
//    while(1){
//        is_handling_S_msg_lock.lock();
//        if(is_handling_S_msg == false){
//            is_handling_S_msg = true;
//            is_handling_S_msg_lock.unlock();
//            break;
//        }
//        is_handling_S_msg_lock.unlock();
//    }
//
//    while(1){
//        is_building_graph_lock.lock();
//        if(is_building_graph == false){
//            is_building_graph_lock.unlock();
//            break;
//        }
//        is_building_graph_lock.unlock();
//    }
//
//
//    while(1){
//        is_running_iteration_lock.lock();
//        if(is_running_iteration == false){
//            is_running_iteration_lock.unlock();
//            break;
//        }
//        is_running_iteration_lock.unlock();
//    }
//
//    int cur_num  = str[0] - '0';
//    if(cur_num != 0 && cur_num != 1){
//        cout << "handle_S_thread_handler: Receive Weird msg 1\n";
//        is_handling_S_msg_lock.lock();
//        is_handling_S_msg = false;
//        is_handling_S_msg_lock.unlock();
//        return;
//    }
//    if(str[1] != 'I'){
//        cout << "handle_S_thread_handler: Receive Weird msg 2.\n";
//        is_handling_S_msg_lock.lock();
//        is_handling_S_msg = false;
//        is_handling_S_msg_lock.unlock();
//        return;
//    }
//
//
//    string temp_str = str.substr(2);    //Skip "I";
//
//    string application_name;
//
//    stringstream iss(temp_str.c_str());
//    iss>>application_name;
//
//    int num_workers = NUM_WORKERS;
//
//    string temp_master_id;
//    iss >> temp_master_id;
//    my_worker_id = stoi(temp_master_id);
//
//    map<int, int> worker_map;       //<worker_id, VM id>
//    int count = 0;
//    string worker_id_str;
//
//    membership_list_lock.lock();
//    cout << "handle_S_thread_handler: Start initing workers\n";
//    while(num_workers > 0){
//        iss >> worker_id_str;
//        int worker_id = stoi(worker_id_str);
//        if(vm_info_map.find(worker_id) == vm_info_map.end()){
//            cout << "handle_S_thread_handler: New workers are not alive\n";
//            break;
//        }
//        else{
//            worker_ip_map[count] = vm_info_map[worker_id].ip_addr_str;
//        }
//        count++;
//        num_workers--;
//    }
//    cout << "handle_S_thread_handler: Finish initing workers\n";
//    master_ip = vm_info_map[my_worker_id].ip_addr_str;
//    membership_list_lock.unlock();
//
//    super_step = 0;
//    is_handling_S_msg_lock.lock();
//    is_handling_S_msg = false;
//    is_handling_S_msg_lock.unlock();
//    return;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_S_msg(int socket_fd, string input_str){
//    string s_msg;
//    int numbytes;
//    char buf[MAX_BUF_LEN];
//    if(input_str == "" ){
//        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
//            cout << "handle_R_msg: Receive error!!\n";
//            return;
//        }
//        s_msg = string(buf, numbytes);
//    }
//    else{
//        s_msg = input_str.substr(1);    //"S..."
//    }
//
//    thread new_thread(handle_S_thread_handler, s_msg);
//    new_thread.detach();
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_B_msg_from_master(int socket_fd, string input_str){
//    is_building_graph_lock.lock();
//    if(is_building_graph == false){
//        is_building_graph = true;
//        is_building_graph_lock.unlock();
//        thread new_thread(build_graph);
//        new_thread.detach();
//    }
//    else{
//        is_building_graph_lock.unlock();
//        cout << "Create duplicate B msg from master. Something is wrong!!\n";
//    }
//}
//
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::handle_R_msg(int socket_fd, string input_str){
//    int numbytes = 0;
//    char buf[MAX_BUF_LEN];
//    string str;
//
//    if(input_str == "" ){
//        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
//            cout << "handle_R_msg: Receive error!!\n";
//            return;
//        }
//        str = string(buf, numbytes);
//    }
//    else{
//        str = input_str.substr(1);      //"R..."
//    }
//
//    int new_super_step = stoi(str);
//    if(new_super_step != super_step +1){
//        cout << "handle_R_msg: ERROR: New super step=" << new_super_step << " cur_super_step = " << super_step << "!!\n";
//        return;
//    }
//    is_running_iteration_lock.lock();
//    if(is_running_iteration == true){
//        is_running_iteration_lock.unlock();
//        cout << "handle_R_msg: Receive R msg while running iteration. Something is WRONG!!\n";
//        return;
//    }
//    is_running_iteration = true;
//    is_running_iteration_lock.unlock();
//    super_step = new_super_step;
//    thread new_thread(run_iteration);
//    new_thread.detach();
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::get_file(){
//    string file_name(INPUT_FILE_NAME);
//    file_name += int_to_string(my_worker_id);
//    read_at_client(file_name, INPUT_GRAPH_LOCAL);
//}
//
//template <typename VertexType, typename MessageValue>
//string Graph<VertexType,MessageValue>::get_master_ip(){
//    return master_ip;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::start_worker_listening_thread(){
//    thread new_thread(worker_listening_thread);
//    new_thread.detach();
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::start_worker_listening_from_master_thread(){
//    thread new_thread(worker_listening_from_master_thread);
//    new_thread.detach();
//}
//
//template <typename VertexType, typename MessageValue>
//int Graph<VertexType,MessageValue>::get_num_vertices_local(){
//    return vertex_map.size();
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::set_total_vertices(int total){
//    total_num_vertices = total;
//}
//
//template <typename VertexType, typename MessageValue>
//int Graph<VertexType,MessageValue>::get_total_num_vertices(){
//    return total_num_vertices;
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::VoteToHalt(string vertex_id){
//    inactive_vertices.insert(vertex_id);
//    active_vertices.erase(vertex_id);
//}
//
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::Reactivate(string vertex_id){
//    inactive_vertices.erase(vertex_id);
//    active_vertices.insert(vertex_id);
//}
//
//
//
//
//
//
//
//
//
