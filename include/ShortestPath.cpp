#include "App.h"
#include "Vertex.h"
#include "Graph.h"


class ShortestPathVertex: public Vertex<int, int, int>{
public:
    ShortestPathVertex(){
        source_id = 1;
    }
    void compute(vector<int>& msgs);
    void init_vertex_val(){
        *MutableValue() = INT_MAX;
    }
    
private:
    int source_id;
};

class App_ShortestPath: public App_Base{
public:
    App_ShortestPath();
    void set_app_info();

private:
    string app_name;
};

App_ShortestPath::App_ShortestPath(){
    app_name = "ShortestPath";
}


void App_ShortestPath::set_app_info(){
    auto ptr = new Graph<ShortestPathVertex, int> (true, true); //weighted, directed
    set_graph_ptr(ptr);
    set_file_name("ShortestPathGraph.txt");
}

void ShortestPathVertex::compute(vector<int>& msgs){
    Reactivate();
    int mindist ;
    if(stoi(get_vertex_id()) == source_id){
        mindist = 0;
    }
    else{
        mindist = INT_MAX;
    }
    for(auto it = msgs.begin(); it != msgs.end() ;it ++){
        mindist = min(mindist, *it);
    }
    if(mindist < GetValue()){
        *MutableValue() = mindist;
        auto it_start = GetOutEdgeIterator_Start();
        auto it_end = GetOutEdgeIterator_End();
        while(it_start != it_end){
            int send_value;
            if(INT_MAX - it_start->second < mindist){
                send_value = INT_MAX;
            }
            else{
                send_value = mindist + it_start->second;
            }
            SendMessageTo(it_start->first, send_value);
            it_start++;
        }
    }
    VoteToHalt();
    return;
}

extern "C" void* create_v_t() {
    return new ShortestPathVertex();
}

extern "C" void destroy_v_t(void* p) {
    App_ShortestPath* p_ = (App_ShortestPath*)p;
    delete p_;
}

extern "C" void* create_a_t() {
    return new App_ShortestPath();
}

extern "C" void destroy_a_t(void* p) {
    ShortestPathVertex* p_ = (ShortestPathVertex*)p;
    delete p_;
}


























