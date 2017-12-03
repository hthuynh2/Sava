#ifndef Graph_h
#define Graph_h

#include "Vertex.h"
#include "UDP.h"
#include "operations.h"
#include "master_scheduler.h"
#define OUTPUT_FILE_NAME "graph_output"


template <typename VertexType, typename MessageValue>
class Graph: public Graph_Base{
public:
    Graph(){};
    Graph(bool is_weighted_, bool is_directed_){
        is_weighted = is_weighted_;
        is_directed = is_directed_;
    };      //<worker_id, VM id>
    void init_graph(map<int,int>& worker_map, int master_vm_num);
    void build_graph();
    bool handle_input_line(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map);
    bool handle_input_line_undirected(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map);

    bool send_all_msg_in_build_buffer(int dest_worker_id, vector<string>& msg_v, map<int,int>& fd_map);
    void send_end_iteration_msg_to_master(bool isDone);
    void set_master_ip(string master_ip_);
    string get_master_ip();
    void set_worker_ip_map(map<int,int>& worker_map);
    int get_num_worker();
    int get_super_step();
    void set_super_step(int super_step_);
    void SendMessageTo(const string& dest_vertex, void* message_ptr) ;
    string create_WM_msg(vector<string>& vertex_v, vector<MessageValue>& msg_value_v);
    string MessageValue_to_String(MessageValue& val);
    void String_to_MessageValue(string& str, MessageValue& val);
    void handle_WB_msg_from_worker(int socket_fd);
    void handle_WM_msg(int socket_fd);
    void handle_MS_thread_handler(string str);
    void handle_MS_msg(int socket_fd, string input_str);
    void handle_MB_msg_from_master(int socket_fd, string input_str);
    void handle_MR_msg(int socket_fd, string input_str);
    void get_file();
    void start_worker_listening_thread();    
    void set_total_vertices(int total);
    int get_total_num_vertices();
    void start_worker_listening_from_master_thread();
    void VoteToHalt(string vertex_id);
    void Reactivate(string vertex_id);
    void run_iteration();
    
    int64_t get_total_send();
    int64_t get_total_receive();

    string get_value_at_vertex(string input_vertex_id);
    int get_num_vertices_local();
    int get_num_edges_local();
    /////
    void Send_all_remain_msg();
    void write_to_file();
    void add_all_vertex_to_set(set<string>& output_container);
    void get_all_vertex_value(map<string,string>& vertex_value_map);
    int get_my_worker_id();
    void write_graph_to_file();
    
    void init_test_info(){
         num_msg_receive_via_network = 0;
         num_msg_send_via_network = 0;
         num_msg_receive_directly = 0;
         num_msg_send_directly = 0;
    }
    
    int64_t get_num_msg_receive_via_network(){
        return num_msg_receive_via_network;
    }
    int64_t get_num_msg_send_via_network(){
        return num_msg_send_via_network;
    }
    int64_t get_num_msg_receive_directly(){
        return num_msg_receive_directly;
    }
    int64_t get_num_msg_send_directly(){
        return num_msg_send_directly;
    }

    
    void Send_all_messages_to(int dest_worker_id);
    void add_value_to_incoming_map(int& map_idx, string& dest_vertex_id_str, string& temp_val);
    

//    typename map<string, EdgeValue>::iterator GetOutEdgeIterator_Start();
//    typename map<string, EdgeValue>::iterator GetOutEdgeIterator_End();
    
    ////
    //
//private:
    std::hash<std::string> str_hash;
    bool is_weighted;
    bool is_directed;
    int num_worker;
    int super_step;
    int my_worker_id;
    string master_ip;
    map<int,string> worker_ip_map;          //<worker_id, worker ip>
    map<string,VertexType> vertex_map;         //<vertex_id, vertex>    //Might need lock while building graph
    map<int , map<string, vector<MessageValue> > > outgoing_msg_map   ;     // <dest_worker_id, <dest_vertex_id, out-msg-buffer> >
//    map<int , int> outgoing_msg_count_map;      //<dest_worker_id, number of msg waiting>
    map<string, vector<MessageValue> > incoming_msg_maps[2];        //<vertex, vector<value> >
    map<string, mutex> incoming_msg_locks[2];

    
    set<string> inactive_vertices;
    set<string> active_vertices;
    bool send_any_msg;
    
    bool is_running_iteration;
    mutex is_running_iteration_lock;
    
    bool is_building_graph;
    mutex is_building_graph_lock;
    
    bool is_handling_S_msg;
    mutex is_handling_S_msg_lock;
    
    int total_num_vertices;
    map<int,int> current_iteration_fd_map;
    
    
    int64_t num_msg_receive_via_network;
    int64_t num_msg_send_via_network;
    int64_t num_msg_receive_directly;
    int64_t num_msg_send_directly;
    


    
//    mutex vertex_map_lock;

//    typename map<string, VertexType>::iterator GetVertexMapIterator_Start();
//    typename map<string, VertexType>::iterator GetVertexMapIterator_End();

//    bool is_stop;           //Need Read write lock!
};




//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::start_building_graph(string file_name){
//    //Split and upload input files
//    File_Manager f_mag;
//    f_mag.split_file(file_name, NUM_WORKERS, INPUT_FILE_NAME);
//    vector<string> local_files_name;
//    for(int i = 0 ; i < NUM_WORKERS; i++){
//        string str(INPUT_FILE_NAME);
//        str += "a";
//        str.push_back((char)('a' + i));
//        local_files_name.push_back(str);
//    }
//
//    for(int i = 0 ; i < (int)file_name.size(); i++){
//        string str = INPUT_FILE_NAME;
//        str += int_to_string(i);
//        write_at_client(local_files_name[i], str);
//    }
//
//    app_client_ptr->start_client_listener_thread();
//    app_client_ptr->send_cb_msg_to_master(-1);
//    return;
//}






////
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::insert_inactive_vertices(string id_){
//    inactive_vertices.insert(id_);
//    active_vertices.erase(id_);
//}
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::delete_inactive_vertices(string id_){
//    inactive_vertices.erase(id_);
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::insert_active_vertices(string id_){
//    active_vertices.insert(id_);
//    inactive_vertices.erase(id_);
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::delete_active_vertices(string id_){
//    active_vertices.erase(id_);
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::delete_all_active_vertices(){
//    active_vertices.erase(active_vertices.begin(), active_vertices.end());
//}
//
//template <typename VertexType, typename MessageValue>
//void Graph<VertexType,MessageValue>::delete_all_inactive_vertices(){
//    inactive_vertices.erase(active_vertices.begin(), active_vertices.end());
//}


//template <typename VertexType, typename MessageValue>
//typename map<string, VertexType>::iterator Graph<VertexType,MessageValue>::GetVertexMapIterator_Start(){
//    return vertex_map.begin();
//}
//
//template <typename VertexType, typename MessageValue>
//typename map<string, VertexType>::iterator Graph<VertexType,MessageValue>::GetVertexMapIterator_End(){
//    return vertex_map.end();
//}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::write_graph_to_file(){
    FILE* fp = fopen("graph_edges", "w");
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        if(it->second.out_going_edges.size() == 0){
            string temp(it->first);
            temp += "\n";
            fwrite(temp.c_str(), sizeof(char), temp.size() , fp);
            continue;
        }
        for(auto it1 = it->second.out_going_edges.begin(); it1 != it->second.out_going_edges.end(); it1++){
            string temp(it->first);
            temp += " "+ it1->first + "\n";
            fwrite(temp.c_str(), sizeof(char), temp.size() , fp);
        }
    }
    fclose(fp);
}


template <typename VertexType, typename MessageValue>
int64_t Graph<VertexType,MessageValue>::get_total_send(){
    int64_t temp = 0;
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        temp += it->second.num_send;
    }
    return temp;
}

template <typename VertexType, typename MessageValue>
int64_t Graph<VertexType,MessageValue>::get_total_receive(){
    int64_t temp = 0;
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        temp += it->second.num_receive;
    }
    return temp;
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_my_worker_id(){
    return my_worker_id;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::get_all_vertex_value(map<string,string>& vertex_value_map){
    for(auto it = vertex_map.begin(); it != vertex_map.end();it++){
        vertex_value_map[it->first]=to_string(it->second.vertex_value);
    }
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::add_all_vertex_to_set(set<string>& output_container){
    for(auto it = vertex_map.begin(); it != vertex_map.end();it++){
        output_container.insert(it->first);
    }
}


template <typename VertexType, typename MessageValue>
string Graph<VertexType,MessageValue>::get_value_at_vertex(string input_vertex_id){
    if(vertex_map.find(input_vertex_id) == vertex_map.end()){
        return "";
    }
    return to_string(vertex_map[input_vertex_id].vertex_value);
}



template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::write_to_file(){
    FILE* fp = fopen(OUTPUT_FILE_NAME, "w");
    string temp("Hello\n");
    fwrite(temp.c_str(), sizeof(char), temp.size() , fp);
    for(auto it = vertex_map.begin(); it!= vertex_map.end(); it++){
        string val_str("");
        val_str += it->second.vertex_id + " ";
        val_str += to_string(it->second.vertex_value) + "\n";
        fwrite(val_str.c_str(), sizeof(char), val_str.size() , fp);
    }
    fclose(fp);
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::init_graph(map<int,int>& worker_map, int master_vm_num){
    membership_list_lock.lock();
    for(auto it = worker_map.begin(); it != worker_map.end(); it++){
        if(vm_info_map.find(it->second) == vm_info_map.end()){
            cout << "Worker is not alive! Something is Wrong!\n";
            membership_list_lock.unlock();
            return;
        }
        else{
            worker_ip_map[it->first] = vm_info_map[it->second].ip_addr_str;
            if(it->second == my_vm_info.vm_num){
                my_worker_id = it->first;
            }
        }
    }
    if(vm_info_map.find(master_vm_num) == vm_info_map.end()){
        cout << "Master is not alive! Something is Wrong!\n";
        return;
    }
    else{
        master_ip = vm_info_map[master_vm_num].ip_addr_str;
    }
    
    membership_list_lock.unlock();
    num_worker = NUM_WORKERS;
    send_any_msg = false;
    super_step = 0;
    is_running_iteration = false;
    is_building_graph = false;
    is_handling_S_msg = false;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::build_graph(){
    cout << "Inside Graph->buildgraph()\n";
    string graph_file(INPUT_GRAPH_LOCAL) ;
    
    set<string> dest_vertex_set;
    set<string> source_vertex_set;
    map<int, vector<string> >  msg_map;           //<worker_id, vector<msg> >
    string line;
    
    //Erase everything!!
//    vertex_map.erase(vertex_map.begin(), vertex_map.end());
//    outgoing_msg_map.erase(outgoing_msg_map.begin(), outgoing_msg_map.end());
//    outgoing_msg_count_map.erase(outgoing_msg_count_map.begin(), outgoing_msg_count_map.end());
//    incoming_msg_maps[0].erase(incoming_msg_maps[0].begin(), incoming_msg_maps[0].end());
//    incoming_msg_maps[1].erase(incoming_msg_maps[1].begin(), incoming_msg_maps[1].end());
//
//    Make connection to all other workers. Send them WB msg
    map<int, int> fd_map;                   //<worker_id, socket>
    string temp_wb_str("WB");
    set<int> open_connection_set;

    for(auto it =  worker_ip_map.begin(); it != worker_ip_map.end(); it++){
        if(it->first == my_worker_id){
            fd_map[it->first] = -1;
            continue;
        }
        int temp_fd = tcp_open_connection(it->second, WORKER_PORT);
        if(temp_fd == -1){
            cout << "build_graph: Cannot make connection\n";
            //Close all  connection
            for(auto it1 = open_connection_set.begin(); it1 != open_connection_set.end(); it1++){
                close(fd_map[*it1]);
            }
            return;
        }
        fd_map[it->first] = temp_fd;
        open_connection_set.insert(it->first);

        if(tcp_send_string(temp_fd, temp_wb_str) == -1){
            //Close all  connection
            for(auto it1 = open_connection_set.begin(); it1 != open_connection_set.end(); it1++){
                if(*it1 > 0)
                    close(fd_map[*it1]);
            }
            return;
        }
    }
    
    cout << "Graph->buildgraph(): Erased old data\n";
    
    ifstream file_stream(graph_file.c_str());
    if (file_stream.is_open()){
        while(getline(file_stream,line)){
            if(line[0] <= '9' && line[0] >= '0' && line.size() > 0){
                if(is_directed){
                    if(handle_input_line(dest_vertex_set, source_vertex_set, line, msg_map, fd_map) == false){
                        cout << "Graph->buildgraph(): Some error happen. Handle_input_line return false\n";
                        return;
                    }
                }
                else{
                    if(handle_input_line_undirected(dest_vertex_set, source_vertex_set, line, msg_map, fd_map) == false){
                        cout << "Graph->buildgraph(): Some error happen. Handle_input_line return false\n";
                        return;
                    }
                }
            }
            //            cout << line << '\n';
        }
        file_stream.close();
    }
    else{
        cout << "Graph->buildgraph: Unable to open file";
    }
    
    cout << "Graph->buildgraph: Send remain source\n";
    for(auto it = msg_map.begin(); it != msg_map.end();it++){
        if(it->second.size() >0){
            if(it->first == my_worker_id){
//                cout << "Graph->buildgraph: send msg to itself. Something is wrong\n";
            }
            else{
//                cout << "Graph->buildgraph: Send remain source\n";
                send_all_msg_in_build_buffer(it->first, it->second, fd_map);
                msg_map[it->first].erase(msg_map[it->first].begin(), msg_map[it->first].end());
            }
        }
    }
//    bool Graph<VertexType,MessageValue>::send_all_msg_in_build_buffer(int dest_worker_id, vector<int>& msg_v){

    
    
    //Send dest_vertex
    if(is_directed){
        cout << "Graph->buildgraph: Start Sending dest\n";
        for(auto it = dest_vertex_set.begin(); it != dest_vertex_set.end() ;it++){
            if(source_vertex_set.find(*it) == source_vertex_set.end()){
                int dest_worker_id = str_hash(*it) % num_worker;
                if(dest_worker_id != my_worker_id){
                    string msg("");
                    msg += *it + "\n";
                    msg_map[dest_worker_id].push_back(msg);
                    if(msg_map[dest_worker_id].size() >= 99 ){
                        //                    send_all_msg_in_build_buffer(fd_map[dest_worker_id], msg_map[dest_worker_id]);
                        //                    cout << "Graph->buildgraph: Add dest msg buffer is full: start sending it\n";
                        send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id], fd_map);
                        msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
                    }
                }
                else{
                    //                cout << "Graph->buildgraph: Add dest vertex to local graph: vertex id =  " << *it <<" \n";
                    
//                    vertex_map_lock.lock();
                    vertex_map[*it].set_vertex_id(*it);
                    vertex_map[*it].init_vertex_val();
                    vertex_map[*it].setGraphPtr((Graph_Base*) this);
//                    vertex_map_lock.unlock();
                }
            }
        }
        
        cout << "Graph->buildgraph: Start Sending remained dest\n";
        for(auto it = msg_map.begin(); it != msg_map.end();it++){
            if(it->second.size() >0){
                if(it->first == my_worker_id){
                    cout << "build graph: send msg to itself. Something is wrong\n";
                }
                else{
                    //                send_all_msg_in_build_buffer(fd_map[it->first], it->second);
                    //                cout << "Graph->buildgraph: Start sending remain dest to : " << it->first << "\n";
                    send_all_msg_in_build_buffer(it->first, it->second, fd_map);
                    msg_map[it->first].erase(msg_map[it->first].begin(), msg_map[it->first].end());
                }
            }
        }
    }
    

    cout << "Graph->buildgraph: Finishing building graph. Send response to master\n";
    string wb_msg("WB");
    if(master_ip != my_vm_info.ip_addr_str){
        int master_fd_temp = tcp_open_connection(master_ip, MASTER_APP_PORT);
        if(master_fd_temp == -1){           //Maybe should wait for new master
            cout << "build graph: Cannot make connection to master\n";
            return;
        }
        tcp_send_string(master_fd_temp, wb_msg);
    }
    else{
        cout << "Graph->buildgraph: Finishing building graph. Send reponse to itself\n";
        master_scheduler_ptr->handle_WB_msg(-1, wb_msg);
    }
    cout << "Graph->buildgraph: DONE\n";
}



template <typename VertexType, typename MessageValue>
bool Graph<VertexType,MessageValue>::handle_input_line_undirected(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map)
{
    if(line[0] > '9' || line[0] < '0'){
        cout << "ERROR: Graph->handle_input_line: " <<  line;
        return false;
    }
    //    cout << "Inside: Graph->handle_input_line: " << line<<"\n";
    stringstream iss(line);
    string source, end, weight;
    
    if(is_weighted == true){
        iss >> source;
        iss >> end;
        iss >> weight;
        
        source_vertex_set.insert(source);
        
        int dest_worker_id = str_hash(source) % num_worker;
        
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
//            vertex_map_lock.lock();
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end, stoi(weight));
                vertex_map[source].setGraphPtr((Graph_Base*) this);
//                vertex_map_lock.unlock();
                
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end, stoi(weight));
            }
        }
        else{
            string msg("");
            msg  += source + " " + end + " " + weight + "\n";
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id],fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
        
        string temp = source;
        source = end;
        end = temp;
        
        source_vertex_set.insert(source);
        
        dest_worker_id = str_hash(source) % num_worker;
        
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end, stoi(weight));
                vertex_map[source].setGraphPtr((Graph_Base*) this);
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end, stoi(weight));
            }
        }
        else{
            string msg("");
            msg  += source + " " + end + " " + weight + "\n";
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id],fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
    }
    else{
        iss >> source;
        iss >> end;
        
        source_vertex_set.insert(source);
        int dest_worker_id = str_hash(source) % num_worker;
        
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
            //            cout << "Graph->handle_input_line: Add vertex to local graph: " << source << " " << end <<"\n";
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end);    //Might be wrong!!!
                vertex_map[source].setGraphPtr((Graph_Base*) this);
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end);    //Might be wrong!!!
            }
        }
        else{
            string msg("");
            msg  += source + " " + end  + "\n";
            //            cout << "Graph->handle_input_line: Add msg to queue " <<msg;
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
                //                cout << "Graph->handle_input_line: Queue is full. Start sending data: \n";
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id], fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                else{
                    //                    cout << "Graph->handle_input_line: Sent all msg in queue to : " << worker_ip_map[dest_worker_id]<<"\n";
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
        
        string temp = source;
        source = end;
        end = temp;
        source_vertex_set.insert(source);
        dest_worker_id = str_hash(source) % num_worker;
        
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
            //            cout << "Graph->handle_input_line: Add vertex to local graph: " << source << " " << end <<"\n";
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end);    //Might be wrong!!!
                vertex_map[source].setGraphPtr((Graph_Base*) this);
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end);    //Might be wrong!!!
            }
        }
        else{
            string msg("");
            msg  += source + " " + end  + "\n";
            //            cout << "Graph->handle_input_line: Add msg to queue " <<msg;
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
                //                cout << "Graph->handle_input_line: Queue is full. Start sending data: \n";
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id], fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                else{
                    //                    cout << "Graph->handle_input_line: Sent all msg in queue to : " << worker_ip_map[dest_worker_id]<<"\n";
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
        
        
    }
    return true;
}



template <typename VertexType, typename MessageValue>
bool Graph<VertexType,MessageValue>::handle_input_line(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map)
{
    if(line[0] > '9' || line[0] < '0'){
        cout << "ERROR: Graph->handle_input_line: " <<  line;
        return false;
    }
//    cout << "Inside: Graph->handle_input_line: " << line<<"\n";
    stringstream iss(line);
    string source, end, weight;
    
    if(is_weighted == true){
        iss >> source;
        iss >> end;
        iss >> weight;
        
        source_vertex_set.insert(source);

        if(source_vertex_set.find(end) == source_vertex_set.end()){
            dest_vertex_set.insert(end);
        }
        
        int dest_worker_id = str_hash(source) % num_worker;
        
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end, stoi(weight));
                vertex_map[source].setGraphPtr((Graph_Base*) this);
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end, stoi(weight));
            }
        }
        else{
            string msg("");
            msg  += source + " " + end + " " + weight + "\n";
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id],fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
    }
    else{
        iss >> source;
        iss >> end;
        
        source_vertex_set.insert(source);
        if(source_vertex_set.find(end) == source_vertex_set.end()){
            dest_vertex_set.insert(end);
        }
        
        int dest_worker_id = str_hash(source) % num_worker;
        if(worker_ip_map[dest_worker_id] == my_vm_info.ip_addr_str){
//            cout << "Graph->handle_input_line: Add vertex to local graph: " << source << " " << end <<"\n";
            if(vertex_map.find(source) == vertex_map.end() ){
                vertex_map[source].set_vertex_id(source);
                vertex_map[source].init_vertex_val();
                vertex_map[source].add_edge(end);    //Might be wrong!!!
                vertex_map[source].setGraphPtr((Graph_Base*) this);
                incoming_msg_locks[0][source].lock();
                incoming_msg_locks[0][source].unlock();
                
                incoming_msg_locks[1][source].lock();
                incoming_msg_locks[1][source].unlock();
            }
            else{
                vertex_map[source].add_edge(end);    //Might be wrong!!!
            }
        }
        else{
            string msg("");
            msg  += source + " " + end  + "\n";
//            cout << "Graph->handle_input_line: Add msg to queue " <<msg;
            msg_map[dest_worker_id].push_back(msg);
            if(msg_map[dest_worker_id].size() >= 99){
                //Send all msg
//                cout << "Graph->handle_input_line: Queue is full. Start sending data: \n";
                if(send_all_msg_in_build_buffer(dest_worker_id, msg_map[dest_worker_id], fd_map) == false){
                    cout << "ERROR: Graph->handle_input_line: fail to send all msg\n";
                    return false;
                }
                else{
//                    cout << "Graph->handle_input_line: Sent all msg in queue to : " << worker_ip_map[dest_worker_id]<<"\n";
                }
                msg_map[dest_worker_id].erase(msg_map[dest_worker_id].begin(), msg_map[dest_worker_id].end());
            }
        }
    }
    return true;
}

template <typename VertexType, typename MessageValue>
bool Graph<VertexType,MessageValue>::send_all_msg_in_build_buffer(int dest_worker_id, vector<string>& msg_v, map<int,int>& fd_map){
//    cout << "Inside: send_all_msg_in_build_buffer: Send to " << worker_ip_map[dest_worker_id]<<"\n";
    if(msg_v.size() > 99){
        cout << "send_all_msg_in_build_buffer: msg_v.size() > 99. Something is WRONG\n";
        return false;
    }
    
    string msg("");
    msg += int_to_string(msg_v.size());
    
    for(int i = 0; i < (int) msg_v.size(); i++){
        msg += msg_v[i];
    }
    
    while(tcp_send_string_with_size(fd_map[dest_worker_id], msg) == -1){    //MIGHT BE WRONG!!
        cout << "send_all_msg_in_build_buffer: Cannot Send for 1st time\n";
        int temp_fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
        if(temp_fd == -1){
            cout << "send_all_msg_in_build_buffer: Cannot make connection\n";
            return false;
        }
        fd_map[dest_worker_id] = temp_fd;
        string temp_wb_str("WB");
        if(tcp_send_string(temp_fd, temp_wb_str) == -1){
            close(temp_fd);
            fd_map[dest_worker_id] = -1;
            return false;
        }
        cout << "send_all_msg_in_build_buffer: " << "made new connection\n";
    }
    

//    cout << "send_all_msg_in_build_buffer: Finish sending msg\n";
    return true;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::run_iteration(){
    cout << "Inside run_iteration\n";
    send_any_msg = false;
    
    
    ////
    if(super_step== 1){
        cout << "run_iteration: First iteration. Set all vertices as active\n";
        inactive_vertices.erase(inactive_vertices.begin(), inactive_vertices.end());
        for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
            active_vertices.insert(it->first);
            inactive_vertices.erase(it->first);
        }
    }
    
    int map_idx = super_step%2;
    cout << "run_iteration: incoming buffer idx = " << map_idx<<"\n";
    
    int temp_count = 0;
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        if((incoming_msg_maps[map_idx].find(it->first) != incoming_msg_maps[map_idx].end()
            && incoming_msg_maps[map_idx][it->first].size() != 0)
           || active_vertices.find(it->first) != active_vertices.end() || inactive_vertices.find(it->first) ==active_vertices.end()){
//            cout << "run_iteration: Start computing vertex id = " << it->first<<"\n";
//            incoming_msg_locks[map_idx][it->first].lock();        //MIGHT NEED THIS
            temp_count+=incoming_msg_maps[map_idx][it->first].size();
            it->second.compute(incoming_msg_maps[map_idx][it->first]);
            
//            incoming_msg_locks[map_idx][it->first].unlock();
//            cout << "run_iteration: Finish computing vertex id = " << it->first<<"\n";
        }
        else{
            cout << "run_iteration: Vertex with if = " << it->first<<" is inactive and not have any msg\n";
        }
    }
    
    
    
   
    std::vector<std::thread> threads;

    for(auto it = outgoing_msg_map.begin(); it != outgoing_msg_map.end(); it ++){
        threads.push_back(thread(start_Send_all_messages_to, it->first, (Graph_Base*) this));
    }
    
    for (auto& th : threads) th.join();

    
    
    ////TESTING
    int temp_count1 = 0;
    
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        if(incoming_msg_maps[map_idx].find(it->first) != incoming_msg_maps[map_idx].end() ){
            temp_count1+=incoming_msg_maps[map_idx][it->first].size();
        }
    }
    
    if(temp_count1 != temp_count){
        cout << "run_iteration: WRONGGGGGGGGG\n\n\n\n\\n\n\n\n\n\n\n\nWRONGGGGGGGGG\n\n\n\n\n\n";
    }
    
    ////TESTING
    
    
    //Erase all used msg
    incoming_msg_maps[map_idx].erase(incoming_msg_maps[map_idx].begin(), incoming_msg_maps[map_idx].end());
    
    if(send_any_msg == false && active_vertices.empty()){
        send_end_iteration_msg_to_master(true);
    }
    else{
        send_end_iteration_msg_to_master(false);
    }
    is_running_iteration_lock.lock();
    is_running_iteration = false;
    is_running_iteration_lock.unlock();
    
 
}

void start_Send_all_messages_to(int dest_worker_id, Graph_Base* graph_ptr){
    graph_ptr->Send_all_messages_to(dest_worker_id);
    return;
}

      
           
template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::send_end_iteration_msg_to_master(bool isDone){
    string msg("WR");
    if(isDone){
        msg += "1";
        msg += to_string(super_step);
    }
    else{
        msg += "0";
        msg += to_string(super_step);
    }
    cout << "Inside send_end_iteration_msg_to_master: Send wr_msg to master msg= " << msg <<"\n";
    
    if(my_vm_info.ip_addr_str !=master_ip ){
        int master_fd = tcp_open_connection(master_ip, MASTER_APP_PORT);
        if(master_fd == -1){
            cout << "send_end_iteration_msg_to_master: Cannot send to master\n";
            return;
        }
        tcp_send_string(master_fd, msg);
        cout << "send_end_iteration_msg_to_master: Sent MR msg to master\n";
    }
    else{
        cout << "send_end_iteration_msg_to_master: Sent MR msg to itself\n";
        master_scheduler_ptr->handle_WR_msg(-1,msg);
    }
    return;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::set_master_ip(string master_ip_){
    master_ip = master_ip_;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::set_worker_ip_map(map<int,int>& worker_map){
    membership_list_lock.lock();
    for(auto it = worker_map.begin(); it != worker_map.end(); it++){
        if(vm_info_map.find(it->first) == vm_info_map.end()){
            cout << "Worker is not alive! Something is Wrong!\n";
        }
        else{
            worker_ip_map[it->first] = vm_info_map[it->first].ip_addr_str;
        }
    }
    membership_list_lock.unlock();
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_num_worker(){
    return (int) worker_ip_map.size();
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_super_step(){
    return super_step;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::set_super_step(int super_step_){
    super_step = super_step_;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::Send_all_messages_to(int dest_worker_id){
    int num_dest_vertex = outgoing_msg_map[dest_worker_id].size();
    
    
    if(num_dest_vertex == 0){
        return ;
    }

    int fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
    if(fd == -1){
        close(fd);
        return ;
    }
    
    string wm_msg("WM");
    if((super_step+1)%2 == 1){
        wm_msg += "1";
    }
    else{
        wm_msg += "0";
    }
    
    string num_dest_vertex_str = to_string(num_dest_vertex);
    if(num_dest_vertex_str.size() < 8){
        int temp_remain = 8 - num_dest_vertex_str.size();
        string new_string("");
        for(int i = 0 ; i < temp_remain; i++){
            new_string += "0";
        }
        new_string += num_dest_vertex_str;
        num_dest_vertex_str = new_string;
    }
    else{
        cout << "OVERFLOW!!!\n";
    }

    
    wm_msg += num_dest_vertex_str;
    if(tcp_send_string(fd, wm_msg) == -1){
        close(fd);
        return ;
    }
    total_send += 8 + 1;

    
    for(auto it = outgoing_msg_map[dest_worker_id].begin(); it != outgoing_msg_map[dest_worker_id].end(); it++){
        string dest_vertex_id = it->first;
        string num_values = to_string(it->second.size()*sizeof(MessageValue));
        string msg("");
        msg += dest_vertex_id + " " + num_values + " ";
        for(int i = 0 ; i < (int)it->second.size() ; i++){
            msg += MessageValue_to_String(it->second[i]);
            if(msg.size() >= 1000){
                if(tcp_send_string(fd, msg) == -1){
                    close(fd);
                    outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end());
                    return ;
                }
                msg = "";
                total_send+=msg.size();
            }
        }
        if(msg.size() > 0){
            if(tcp_send_string(fd,msg) == -1){
                close(fd);
                outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end()) ;
                return ;
            }
            total_send+=msg.size();
        }
    }
    
    
    int numbytes;
    char buf[1];
    //NEED TO DO: Might need to setsockopt!!!
    if((numbytes = recv(fd, buf, 1, 0)) <= 0 ){
        close(fd);
        outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end()) ;
        return ;
    }
    else{
        close(fd);
        outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end()) ;
        return ;
    }
}


void handle_value_msg(int map_idx, int dest_vertex_id, string str, Graph_Base* graph_ptr, int size_Msg_val){
    if(str.size() % size_Msg_val != 0 ){
        cout << "handle_value_msg: Somethign is WRONG\n";
    }
    int num_values = str.size() / size_Msg_val;
    int offset = 0;
    
    string dest_vertex_id_str = to_string(dest_vertex_id);
    
    while(num_values > 0){
        string temp_value_str = str.substr(offset, size_Msg_val);
        offset += size_Msg_val;
        graph_ptr->add_value_to_incoming_map(map_idx, dest_vertex_id_str, temp_value_str);
        num_values--;
    }
    return;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_WM_msg(int socket_fd){
    //NEED TO DO: MIGHT NEED TO SETSOCKOPT
    cout <<"Inside handle_WM_msg\n";
    int map_idx;
    int num_dest_vertex;
    int numbytes;
    char buf[MAX_BUF_LEN];
    
    
    total_receive += 2;
    if((numbytes = recv(socket_fd, buf, 1, 0) ) <= 0 ){
        cout << "handle_WM_msg: ERROR: Cannot receive 0a\n";
        close(socket_fd);
        return ;
    }
    else{
        map_idx = buf[0] - '0';
        total_receive++;
    }
    cout << "handle_WM_msg: map_idx = "<<map_idx<<"\n";
    if((numbytes = recv(socket_fd, buf, 8, 0) ) < 8 ){
        cout << "handle_WM_msg: ERROR:Cannot receive 0\n";
        close(socket_fd);
        return ;
    }
    else{
        string temp(buf, numbytes);
        num_dest_vertex = stoi(temp);
        total_receive+=8;
    }
    cout << "handle_WM_msg: num_dest_vertex = "<<num_dest_vertex<<"\n";

//    std::vector<std::thread> threads;
    while(num_dest_vertex > 0){
        int dest_vertex_id = 0;
        while(1){
            char c;
            if((recv(socket_fd, &c, 1, 0)) <= 0){
                cout << "handle_WM_msg:ERROR: Cannot receive 1\n";
                close(socket_fd);

                return ;
            }
            total_receive++;
            if(c == ' ')
                break;
            dest_vertex_id = dest_vertex_id*10 + (c - '0');
        }
        
        int need_to_read_bytes = 0;
        while(1){
            char c;
            if((recv(socket_fd, &c, 1, 0)) <= 0){
                cout << "handle_WM_msg:ERROR: Cannot receive 2\n";
                close(socket_fd);
                return ;
            }
            total_receive++;
            if(c == ' ')
                break;
            need_to_read_bytes = need_to_read_bytes*10 + (c - '0');
        }
        
        
        //Get msg
        char* buf1 = (char*) malloc(need_to_read_bytes);
        int numbyte;
        int offset = 0;
        int msg_len = need_to_read_bytes;
        while(msg_len > 0){
            int numbyte = recv(socket_fd,(void*) (buf1 + offset), msg_len, 0);
            if(numbyte == -1){
                cout << "handle_WM_msg:ERROR: Cannot receive 3\n";
                close(socket_fd);
                return;
            }
            total_receive += numbyte;
            msg_len -= numbyte;
            offset += numbyte;
        }
        string str(buf1, need_to_read_bytes);
//        cout << "handle_WM_msg: Start creating thread for dest_id = " << dest_vertex_id << "\n";
        
        
        if(str.size() % sizeof(MessageValue) != 0 ){
            cout << "handle_value_msg: ERROR: Somethign is WRONG\n";
        }
        int num_values = str.size() / sizeof(MessageValue);
        int offset1 = 0;
        
        string dest_vertex_id_str = to_string(dest_vertex_id);
        while(num_values > 0){
            string temp_value_str = str.substr(offset1, sizeof(MessageValue));
            offset1 += sizeof(MessageValue);
            add_value_to_incoming_map(map_idx, dest_vertex_id_str, temp_value_str);
            num_values--;
        }

        free(buf1);
        num_dest_vertex--;
    }
    cout << "handle_WM_msg: DONE WITH num_dest_vertex = "<<num_dest_vertex<<"\n";

    string reply("A");
    tcp_send_string(socket_fd, reply);
    close(socket_fd);
    return ;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::add_value_to_incoming_map(int& map_idx, string& dest_vertex_id_str, string& temp_value_str){
    MessageValue temp_val;
    String_to_MessageValue(temp_value_str, temp_val);
    incoming_msg_locks[map_idx][dest_vertex_id_str].lock();
    incoming_msg_maps[map_idx][dest_vertex_id_str].push_back(temp_val);
    num_msg_receive_via_network++;
    incoming_msg_locks[map_idx][dest_vertex_id_str].unlock();
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::Send_all_remain_msg(){
    cout << "Send_all_remain_msg: Start \n";
    for(auto it = outgoing_msg_map.begin(); it != outgoing_msg_map.end() ;it++){
//        cout << "Send_all_remain_msg: Here1 \n";
        int dest_worker_id = it->first;
        if(dest_worker_id == my_worker_id){
            cout << "Send_all_remain_msg: Send to itself. Something it wrong\n";
        }
        else if(it->second.size() > 0){
//            cout << "Send_all_remain_msg: Here2 \n";

            send_any_msg = true;
            vector<string> vertex_v;
            vector<MessageValue> msg_value_v;
            for(auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
//                cout << "Send_all_remain_msg: Here3 \n";

                for(auto it1 = it2->second.begin(); it1 != it2->second.end(); it1++){
//                    cout << "Send_all_remain_msg: Here4 \n";
//                    map<int , map<string, vector<MessageValue> > > outgoing_msg_map   ;     // <dest_worker_id, <dest_vertex_id, out-msg-buffer> >
//                    if(it2 == NULL){
//                        cout << "Send_all_remain_msg: Here4a \n";
//                    }
                    string t = it2->first;
//                    cout << "Send_all_remain_msg: Here4b : " << t<<"\n";
                    vertex_v.push_back(t);
//                    cout << "Send_all_remain_msg: Here5 \n";

                    msg_value_v.push_back(*it1);
//                    cout << "Send_all_remain_msg: Here6 : " << *it1<<" \n";
                }
            }
            
            string msg_m = create_WM_msg(vertex_v, msg_value_v);
//            cout << "Send_all_remain_msg: "<< msg_m<<"\n";
            while(tcp_send_string_with_size(current_iteration_fd_map[dest_worker_id], msg_m) == -1){    //MIGHT BE WRONG!!
                int temp_fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
                if(temp_fd == -1){
                    cout << "SendMessageTo: Cannot make connection\n";
                    return ;
                }
                current_iteration_fd_map[dest_worker_id] = temp_fd;
                string temp_wm_str("WM");
                if(tcp_send_string(temp_fd, temp_wm_str) == -1){
                    close(temp_fd);
                    current_iteration_fd_map[dest_worker_id] = -1;
                    return ;
                }
                cout << "SendMessageTo: " << "made new connection\n";
            }
//            cout << "Send_all_remain_msg: Here7 \n";

            outgoing_msg_map[it->first].erase(outgoing_msg_map[it->first].begin(), outgoing_msg_map[it->first].end());
//            cout << "Send_all_remain_msg: Here8 \n";

//            outgoing_msg_count_map[dest_worker_id] = 0;
//            cout << "Send_all_remain_msg: Here9 \n";
        }
//        cout << "Send_all_remain_msg: Here10 \n";

    }
    cout << "Send_all_remain_msg: Done \n";

}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::SendMessageTo(const string& dest_vertex, void* message_ptr){
    if(message_ptr == NULL){
        cout << "SendMessageTo: msg_ptr =NULL\n";
        return;
    }
    MessageValue* msg_ptr = (MessageValue*) message_ptr;
    MessageValue msg = * msg_ptr;
    
    send_any_msg = true;
    int dest_worker_id = str_hash(dest_vertex) % num_worker;
    string dest_worker_ip = worker_ip_map[dest_worker_id];
//    cout <<"SendMessageTo: dest_id = " << dest_vertex << " || msg = " << msg<<"\n";

    if(dest_worker_id == my_worker_id){
        //MOVE MSG TO MSG QUEUE
        int map_idx = (super_step + 1) %2;
        incoming_msg_locks[map_idx][dest_vertex].lock();
        incoming_msg_maps[map_idx][dest_vertex].push_back(msg);
        num_msg_send_directly ++;
        num_msg_receive_directly ++;
        incoming_msg_locks[map_idx][dest_vertex].unlock();
        return;
    }
 
    //    map<string, vector<MessageValue> > outgoing_msg_map   ;     //<dest_vertex_id, out-msg-buffer>
    
    num_msg_send_via_network++;
    outgoing_msg_map[dest_worker_id][dest_vertex].push_back(msg);
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
//        string msg_m = create_WM_msg(vertex_v, msg_value_v);
//        ////
//        while(tcp_send_string_with_size(current_iteration_fd_map[dest_worker_id], msg_m) == -1){    //MIGHT BE WRONG!!
//            int temp_fd = tcp_open_connection(worker_ip_map[dest_worker_id], WORKER_PORT);
//            if(temp_fd == -1){
//                cout << "SendMessageTo: Cannot make connection\n";
//                return ;
//            }
//            current_iteration_fd_map[dest_worker_id] = temp_fd;
//            string temp_wm_str("WM");
//            if(tcp_send_string(temp_fd, temp_wm_str) == -1){
//                close(temp_fd);
//                current_iteration_fd_map[dest_worker_id] = -1;
//                return ;
//            }
//            cout << "SendMessageTo: " << "made new connection\n";
//        }
//
//        outgoing_msg_map[dest_worker_id].erase(outgoing_msg_map[dest_worker_id].begin(), outgoing_msg_map[dest_worker_id].end());
//        outgoing_msg_count_map[dest_worker_id] = 0;
//    }
}

template <typename VertexType, typename MessageValue>
string Graph<VertexType,MessageValue>::create_WM_msg(vector<string>& vertex_v, vector<MessageValue>& msg_value_v){
    //    string msg("WM");
    string msg("");
    
    if(vertex_v.size() > 99 || vertex_v.size() == 0){
        cout << "create_M_msg: ERROR: vertex_v has size > 99 or empty!!!!. Size = " << vertex_v.size() << "\n";
        return msg;
    }
    
    if((super_step +1)%2 == 0){
        msg += "0";
    }
    else{
        msg += "1";
    }

    //    msg += to_string(vertex_v.size());
    msg += int_to_string(vertex_v.size());
    
    for(int i = 0 ; i < (int) vertex_v.size() ;i++){
        msg += vertex_v[i] ;
        if(i != (int)vertex_v.size() -1){
            msg += " ";
        }
        else{
            msg += "\n";
        }
//        cout << "create_WM_msg: vertex_v...: " << vertex_v[i]<<"\n";
    }
    
    for(int i = 0; i < (int) msg_value_v.size(); i++){
        string msg_val_str = MessageValue_to_String(msg_value_v[i]);
        msg += msg_val_str;
//        cout << "create_WM_msg: message_v...: " << msg_value_v[i]<<"\n";
    }
    msg += "\n";
//    cout <<"create_WM_msg: " << msg;
    //No '\n' at the end!!!!
    return msg;
}

template <typename VertexType, typename MessageValue>
string Graph<VertexType,MessageValue>::MessageValue_to_String(MessageValue& val){
    char buf[sizeof(MessageValue)];
    memcpy((void*)(buf), (void*)(&val), sizeof(MessageValue));
    string str(buf, sizeof(MessageValue));
    return str;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::String_to_MessageValue(string& str, MessageValue& val){
    if(str.size() != sizeof(MessageValue)){
        cout << "String_to_MessageValue: Str has size " << str.size() << "\n";
        return;
    }
    memcpy((void*)(&val), (void*)(str.c_str()), sizeof(MessageValue));
}



template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_WB_msg_from_worker(int socket_fd){
    cout << "Inside handle_WB_msg_from_worker\n";
    //NEED TO DO: Set sockopt.
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 30;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    
    while(1){
        string msg = tcp_receive_str(socket_fd);
        if(msg.size() < 2){
            close(socket_fd);
            cout << "handle_WB_msg_from_worker: Cannot receive. Return\n";
            return;
        }
        if(msg[0] > '9' || msg[0] < '0' ||msg[1] > '9' || msg[1] < '0' ){
            cout << "Inside handle_WB_msg_from_worker: Error with number vertices\n";
            close(socket_fd);
            return;
        }
        
        int num_vertices = 0;
        num_vertices = (msg[0] - '0')*10 + (msg[1] - '0');
        if(num_vertices <= 0){
            cout << "handle_WB_msg_from_worker: Number of vertices = 0. Something is wrong" <<"\n";
            close(socket_fd);
            continue;
        }
        else{
//            cout << "handle_WB_msg_from_worker: Number of vertices = " << num_vertices <<"\n";
        }
        string str = msg.substr(2);
        
        std::stringstream ss(str.c_str());
        std::string temp_str;
        while(getline(ss, temp_str, '\n')){
            stringstream iss(temp_str.c_str());
            string source , end, weight;
            
            iss >> source;
            iss >> end;
            iss >> weight;
//            cout << "handle_WB_msg_from_worker: " << source << " " << end << " " << weight<<"\n";
            vertex_map[source].set_vertex_id(source);
            vertex_map[source].init_vertex_val();
            vertex_map[source].setGraphPtr((Graph_Base*) this);
            incoming_msg_locks[0][source].lock();
            incoming_msg_locks[0][source].unlock();
            
            incoming_msg_locks[1][source].lock();
            incoming_msg_locks[1][source].unlock();
            if(end.size() == 0 && weight.size() == 0){
//                cout << "handle_WB_msg_from_worker: receive just source\n";
                num_vertices --;
                continue;
            }
            if(is_weighted == false){
//                cout << "handle_WB_msg_from_worker: Added edge with source, end = " << source << ", " << end <<"\n";
                vertex_map[source].add_edge(end);
                num_vertices --;
            }
            else{
                if(weight.size() == 0){
                    cout << "Inside handle_B_msg: Weight.size() == 0. Something is WRONG!!!!\n";
                    close(socket_fd);
                    return;
                }
                vertex_map[source].add_edge(end, stoi(weight));
                num_vertices --;
            }
        }
        if(num_vertices != 0 ){
            cout << "handle_WB_msg_from_worker: Number of vertices is NOT 0. Something is wrong" <<"\n";
            close(socket_fd);
            return;
        }
//        cout << "Inside handle_WB_msg_from_worker:DONE!!!\n";
    }
}

//
//void worker_listening_thread(Graph_Base* graph_ptr){
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
//        thread new_thread(worker_handler_thread, new_fd, graph_ptr);
//        new_thread.detach();
//    }
//}

//
//void worker_handler_thread(int socket_fd, Graph_Base* graph_ptr){
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
//        graph_ptr->handle_B_msg_from_worker(socket_fd);
//    }
//    else if(buf[0] == 'M'){
//        graph_ptr->handle_M_msg(socket_fd);
//    }
//    else{
//        cout << "Inside worker_handler_thread: Receive Undefined msg. Something is WRONG!!!\n";
//    }
//    close(socket_fd);
//    return;
//}


//
//void worker_listening_from_master_thread(Graph_Base* graph_ptr){         //Listen msg from master
//    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
//    struct addrinfo hints, *servinfo, *p;
//    struct sockaddr_storage their_addr; // connector's address information
//    socklen_t sin_size;
//
//    int worker_port_int= stoi(WORKER_MASTER_PORT);
//    string my_worker_port = to_string(my_port_offset + worker_port_int);
//    //    cout << "Stab listening on port "<< my_stab_port<<"\n";
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
//        thread new_thread(handle_master_msg_thread, new_fd, graph_ptr);
//        new_thread.detach();
//    }
//}
//
//
//void handle_master_msg_thread(int socket_fd, Graph_Base* graph_ptr){
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
//        graph_ptr->handle_R_msg(socket_fd, "");
//    }
//    else if(buf[0] == 'B'){
//        graph_ptr->handle_B_msg_from_master(socket_fd, "");
//    }
//    else if(buf[0] == 'S'){
//        //Stop all threads that is running!!!!!
//        graph_ptr->handle_S_msg(socket_fd, "");
//    }
//    else if(buf[0] == 'G'){
//        string msg = to_string(graph_ptr->get_num_vertices_local());
//        tcp_send_string(socket_fd, msg);
//        return;
//    }
//    else if(buf[0] == 'V'){
//        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
//            cout << "handle_master_msg_thread: Cannot receive msg\n";
//        }
//        else{
//            string str(buf,numbytes);
//            graph_ptr->set_total_vertices(stoi(str));
//        }
//    }
//    else{
//        cout << "Inside handle_master_msg_thread: Receive Undefined msg. Something is WRONG!!!\n";
//    }
//    close(socket_fd);
//    return;
//}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_MS_thread_handler(string str){   //"1...." or "0..."
    
    //Note: Might need to also stop receiving all msg from other workers
    while(1){
        is_handling_S_msg_lock.lock();
        if(is_handling_S_msg == false){
            is_handling_S_msg = true;
            is_handling_S_msg_lock.unlock();
            break;
        }
        is_handling_S_msg_lock.unlock();
    }
    
    while(1){
        is_building_graph_lock.lock();
        if(is_building_graph == false){
            is_building_graph_lock.unlock();
            break;
        }
        is_building_graph_lock.unlock();
    }
    
    
    while(1){
        is_running_iteration_lock.lock();
        if(is_running_iteration == false){
            is_running_iteration_lock.unlock();
            break;
        }
        is_running_iteration_lock.unlock();
    }
    
    int cur_num  = str[0] - '0';
    if(cur_num != 0 && cur_num != 1){
        cout << "handle_MS_thread_handler: Receive Weird msg 1\n";
        is_handling_S_msg_lock.lock();
        is_handling_S_msg = false;
        is_handling_S_msg_lock.unlock();
        return;
    }
    
    string temp_str = str.substr(1);
    
    stringstream iss(temp_str.c_str());
    
    int num_workers = NUM_WORKERS;
    
    string temp_master_id;
    iss >> temp_master_id;
    
    map<int, int> worker_map;       //<worker_id, VM id>
    int count = 0;
    string worker_id_str;
    
    membership_list_lock.lock();
    cout << "handle_MS_thread_handler: Start initing workers\n";
    while(num_workers > 0){
        iss >> worker_id_str;
        int worker_id = stoi(worker_id_str);
        if(vm_info_map.find(worker_id) == vm_info_map.end()){
            cout << "handle_MS_thread_handler: New workers are not alive\n";
            break;
        }
        else{
            worker_ip_map[count] = vm_info_map[worker_id].ip_addr_str;
        }
        count++;
        num_workers--;
    }
    cout << "handle_MS_thread_handler: Finish initing workers\n";
    master_ip = vm_info_map[stoi(temp_master_id)].ip_addr_str;
    membership_list_lock.unlock();
    
    super_step = 0;
    is_handling_S_msg_lock.lock();
    is_handling_S_msg = false;
    is_handling_S_msg_lock.unlock();
    return;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_MS_msg(int socket_fd, string input_str){
    string ms_msg;
    int numbytes;
    char buf[MAX_BUF_LEN];
    if(input_str == "" ){
        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
            cout << "handle_R_msg: Receive error!!\n";
            return;
        }
        ms_msg = string(buf, numbytes);
    }
    else{
        ms_msg = input_str.substr(2);    //"MS..."
    }
    
    thread new_thread(run_handle_MS_thread_handler_thread,(Graph_Base*)this, ms_msg);
    new_thread.detach();
}
//
//void run_handle_MS_thread_handler_thread(Graph_Base* graph_ptr, string s_msg){
//    graph_ptr->handle_S_thread_handler(s_msg);
//}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_MB_msg_from_master(int socket_fd, string input_str){
    cout << "Inside handle_MB_msg_from_master: "<< input_str<<"\n";
    is_building_graph_lock.lock();
    if(is_building_graph == false){
        cout << "Start building graph\n";
        is_building_graph = true;
        is_building_graph_lock.unlock();
        thread new_thread(start_build_graph_thread, (Graph*) this);
        new_thread.detach();
    }
    else{
        is_building_graph_lock.unlock();
        cout << "Create duplicate B msg from master. Something is wrong!!\n";
    }
}

//void start_build_graph_thread(Graph_Base* graph_ptr){
////    graph_ptr->build_graph();
//}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::handle_MR_msg(int socket_fd, string input_str){
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    string str;
    cout << "Inside handle_MR_msg\n";
    
    if(input_str == "" ){
        if((numbytes = recv(socket_fd, buf,MAX_BUF_LEN, 0)) <= 0 ){
            cout << "handle_MR_msg: Receive error!!\n";
            return;
        }
        str = string(buf, numbytes);
    }
    else{
        str = input_str.substr(2);      //"MR..."
    }
    
    cout << "handle_MR_msg: msg = " << str <<"\n";
    int new_super_step = stoi(str);
    cout << "handle_MR_msg: receive super_step_num = " << new_super_step <<"\n";
    if(new_super_step != super_step +1){
        cout << "handle_MR_msg: ERROR: New super step=" << new_super_step << " cur_super_step = " << super_step << "!!\n";
        return;
    }
    is_running_iteration_lock.lock();
    if(is_running_iteration == true){
        is_running_iteration_lock.unlock();
        cout << "handle_MR_msg: Receive R msg while running iteration. Something is WRONG!!\n";
        return;
    }
    is_running_iteration = true;
    is_running_iteration_lock.unlock();
    super_step = new_super_step;
    cout << "handle_MR_msg: Start new iteration with superstep = " << super_step <<"\n";

    thread new_thread(start_run_iteration_thread, (Graph_Base*) this);
    new_thread.detach();
}



template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::get_file(){
    string file_name(INPUT_FILE_NAME);
    file_name += int_to_string(my_worker_id);
    read_at_client(file_name, INPUT_GRAPH_LOCAL);
}

template <typename VertexType, typename MessageValue>
string Graph<VertexType,MessageValue>::get_master_ip(){
    return master_ip;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::start_worker_listening_thread(){
    thread new_thread(worker_listening_thread, (Graph_Base*)this);
    new_thread.detach();
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::start_worker_listening_from_master_thread(){
    thread new_thread(worker_listening_from_master_thread, (Graph_Base*) this);
    new_thread.detach();
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_num_vertices_local(){
    return vertex_map.size();
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_num_edges_local(){
    int total_edge = 0;
    for(auto it = vertex_map.begin(); it != vertex_map.end(); it++){
        total_edge += it->second.out_going_edges.size();
    }
    
    return total_edge;
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::set_total_vertices(int total){
    total_num_vertices = total;
}

template <typename VertexType, typename MessageValue>
int Graph<VertexType,MessageValue>::get_total_num_vertices(){
    return total_num_vertices;
}

template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::VoteToHalt(string vertex_id){
    inactive_vertices.insert(vertex_id);
    active_vertices.erase(vertex_id);
}


template <typename VertexType, typename MessageValue>
void Graph<VertexType,MessageValue>::Reactivate(string vertex_id){
    inactive_vertices.erase(vertex_id);
    active_vertices.insert(vertex_id);
}
















//
#endif












