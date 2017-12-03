#ifndef Vertex_h
#define Vertex_h

#include "common.h"


class Graph_Base{
public:
    virtual ~Graph_Base() {};
    virtual void run_iteration()= 0 ;
    virtual void build_graph()= 0;
    virtual bool handle_input_line(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map)= 0;
    virtual bool handle_input_line_undirected(set<string>& dest_vertex_set, set<string>& source_vertex_set, string& line, map<int, vector<string> >&  msg_map, map<int,int>& fd_map)= 0;

    virtual bool send_all_msg_in_build_buffer(int dest_worker_id, vector<string>& msg_v, map<int,int>& fd_map)= 0;
    virtual void send_end_iteration_msg_to_master(bool isDone)= 0;
    virtual void set_master_ip(string master_ip_)= 0;
    virtual void set_worker_ip_map(map<int,int>& worker_map)= 0;
    virtual int get_num_worker()= 0;
    virtual int get_super_step()= 0;
    virtual void set_super_step(int super_step_)= 0;
    virtual void SendMessageTo(const string& dest_vertex, void* message_ptr) = 0;
    virtual void handle_WB_msg_from_worker(int socket_fd)= 0;
    virtual void handle_MS_thread_handler(string str)= 0;
    virtual void handle_MS_msg(int socket_fd, string input_str)= 0;
    virtual void handle_MB_msg_from_master(int socket_fd, string input_str)= 0;
    virtual void handle_MR_msg(int socket_fd, string input_str)= 0;
    virtual void get_file()= 0;
    virtual string get_master_ip()= 0;
    virtual void start_worker_listening_thread()= 0;
    virtual int get_num_vertices_local() = 0;
    virtual void set_total_vertices(int total) = 0;
    virtual int get_total_num_vertices() = 0;
    virtual void start_worker_listening_from_master_thread() = 0;
    virtual void VoteToHalt(string vertex_id) = 0;
    virtual void Reactivate(string vertex_id) = 0;
    virtual void init_graph(map<int,int>& worker_map, int master_vm_num) = 0;
     virtual void handle_WM_msg(int socket_fd) = 0;
    virtual void  Send_all_remain_msg() = 0;
    virtual void write_to_file() = 0;
    virtual string get_value_at_vertex(string input_vertex_id) =0;
    virtual void add_all_vertex_to_set(set<string>& output_container) = 0;
    virtual void get_all_vertex_value(map<string,string>& vertex_value_map) = 0;
    virtual int get_my_worker_id() = 0;

    virtual int64_t get_total_send() = 0;
    virtual int64_t get_total_receive() = 0;
    virtual int get_num_edges_local() = 0;

//    virtual string create_WM_msg(vector<string>& vertex_v, vector<MessageValue>& msg_value_v) = 0;
};

template   <class VertexValue,
            class EdgeValue,
            class MessageValue>
class Vertex{
public:
    virtual void compute(vector<MessageValue>& msgs) = 0;
    virtual ~Vertex(){};
    void setGraphPtr(Graph_Base* ptr);
    virtual void init_vertex_val() = 0;

    
    int superstep() const;
    Vertex(string vertex_id_);
    Vertex(){};
    
    const string& get_vertex_id() const;
//    string* MutableId();
    void set_vertex_id(string vertex_id_);

    const VertexValue& GetValue() const;
    VertexValue* MutableValue();
    
    Graph_Base** MutableGraph_ptr();
    
    typename map<string, EdgeValue>::iterator GetOutEdgeIterator_Start();
    typename map<string, EdgeValue>::iterator GetOutEdgeIterator_End();
    int get_out_edge_size();
    
    void SendMessageTo(const string& dest_vertex, const MessageValue& message);
    void VoteToHalt();
    void Reactivate(){
        graph_ptr->Reactivate(vertex_id);
    }
    
    void add_edge(string dest, EdgeValue weight);
    void add_edge(string dest);
    void send_to_all_neighbors(MessageValue& value);
    
    int64_t num_receive;
    int64_t num_send;

    
//protected:
    string vertex_id;
    map<string, EdgeValue> out_going_edges;
    EdgeValue default_edge_val;
    VertexValue vertex_value;
    Graph_Base *graph_ptr;
};


template <class VertexValue, class EdgeValue, class MessageValue>
Vertex<VertexValue, EdgeValue, MessageValue>::Vertex(string vertex_id_){
    vertex_id = vertex_id_;
}

template <class VertexValue, class EdgeValue, class MessageValue>
const string& Vertex<VertexValue, EdgeValue, MessageValue>::get_vertex_id() const{
    return vertex_id;
}

template <class VertexValue, class EdgeValue, class MessageValue>
const VertexValue& Vertex<VertexValue, EdgeValue, MessageValue>::GetValue() const{
    return vertex_value;
}

template <class VertexValue, class EdgeValue, class MessageValue>
VertexValue* Vertex<VertexValue, EdgeValue, MessageValue>::MutableValue(){
    return &vertex_value;
}

template <class VertexValue, class EdgeValue, class MessageValue>
Graph_Base** Vertex<VertexValue, EdgeValue, MessageValue>::MutableGraph_ptr(){
    return &graph_ptr;
}

template <class VertexValue, class EdgeValue, class MessageValue>
class map<string, EdgeValue>::iterator Vertex<VertexValue, EdgeValue, MessageValue>::GetOutEdgeIterator_Start(){
    return out_going_edges.begin();
}

template <class VertexValue, class EdgeValue, class MessageValue>
class map<string, EdgeValue>::iterator Vertex<VertexValue, EdgeValue, MessageValue>::GetOutEdgeIterator_End(){
    return out_going_edges.end();
}

template <class VertexValue, class EdgeValue, class MessageValue>
int Vertex<VertexValue, EdgeValue, MessageValue>::get_out_edge_size(){
    return out_going_edges.size();
}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::SendMessageTo(const string& dest_vertex, const MessageValue& message){
    if(graph_ptr == NULL){
        cout << "Vertex->SendMessageTo: ERORR :graph_ptr = NULL\n";
        return;
    }
    graph_ptr->SendMessageTo(dest_vertex, (void*) &message);
//    void SendMessageTo(const string& dest_vertex, void* message_ptr) ;
}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::VoteToHalt(){
    graph_ptr->VoteToHalt(vertex_id);
}

//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::Reactivate(){
//    graph_ptr->Reactivate(vertex_id);
//}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::add_edge(string dest, EdgeValue weight){
    //Might need lock
    if(out_going_edges.find(dest) != out_going_edges.end()){
        cout << "Vertex: add duplicate edge! Something is WRONG!\n";
        return;
    }
    out_going_edges[dest] = weight;
}

//VertexValue* Vertex::MutableId(){
//    return &vertex_id;
//}
template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::set_vertex_id(string vertex_id_){
    vertex_id = vertex_id_;
}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::add_edge(string dest){
    out_going_edges[dest] = default_edge_val;
}


template <class VertexValue, class EdgeValue, class MessageValue>
int Vertex<VertexValue, EdgeValue, MessageValue>::superstep() const{
    return graph_ptr->get_super_step();
}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::send_to_all_neighbors(MessageValue& value){
//    cout << "Vertex->send_to_all_neighbors: Start sending to all neighbors: value = "<< value <<"\n";
    if(out_going_edges.size() == 0){
//        cout << "Vertex->send_to_all_neighbors: There is not neighbors\n";
        return;
    }
    for(auto it = out_going_edges.begin(); it!= out_going_edges.end(); it++){
        SendMessageTo(it->first, value);
    }
//    cout << "Vertex->send_to_all_neighbors: DONE\n";
    return;
}

template <class VertexValue, class EdgeValue, class MessageValue>
void Vertex<VertexValue, EdgeValue, MessageValue>::setGraphPtr(Graph_Base* ptr){
    graph_ptr = ptr;
}





#endif



