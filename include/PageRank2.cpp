#include "App.h"
#include "Vertex.h"
#include "Graph.h"

//#define FILE_NAME "PageRankGraph.txt"


class PageRankVertex: public Vertex<double, int, double>{
public:
    PageRankVertex(){num_receive = 0; num_send = 0;};
    void compute(vector<double>& msgs);
    void init_vertex_val(){
        *MutableValue() = 1;
    }
};

class App_PageRank: public App_Base{
public:
    App_PageRank();
    void set_app_info();
    void Output_function(map<string,string>& vertex_value_map);

private:
    string app_name;
};

App_PageRank::App_PageRank(){
    app_name = "PageRank";
//    set_graph_ptr(NULL);
}


void App_PageRank::set_app_info(){
    auto ptr = new Graph<PageRankVertex, double> (false, true); //unweighted,is_directed
    set_graph_ptr(ptr);
    set_file_name("PageRankGraph.txt");
}

void App_PageRank::Output_function(map<string,string>& vertex_value_map){
    typedef pair<double, int> Pair;
    typedef set<Pair> Set_t;
    Set_t top_25_set;
    
    
    
    for(auto it = vertex_value_map.begin(); it != vertex_value_map.end(); it++){
        int vert_idx = stoi(it->first);
        double val = atof(it->second.c_str());
        Pair temp_p;
        temp_p.first = val;
        temp_p.second = vert_idx;
        top_25_set.insert(temp_p);
        if(top_25_set.size() > 25){
            top_25_set.erase(top_25_set.begin());
        }
    }
    
    for(auto it = top_25_set.begin(); it != top_25_set.end(); it++){
        Add_to_Output_Container(to_string((*it).second), to_string((*it).first));
    }
}

void PageRankVertex::compute(vector<double>& msgs){
    Reactivate();
    if(superstep() >= 1){
        double sum = 0;
        for(auto it = msgs.begin(); it != msgs.end(); it++){
            sum += *it;
            //
            num_receive ++;
            //
        }
        *MutableValue() += sum;
        *MutableValue() = 0.15 + 0.85*sum;
        
        //        *MutableValue() = (double)(0.15/(double)(graph_ptr->get_total_num_vertices()))  + 0.85*sum;
    }
    
    if(superstep() <= 20){
        int num_out_edges = out_going_edges.size();
        //
        num_send += num_out_edges;
        
        //
        double send_value = GetValue()/(double)num_out_edges;
//        double send_value = stoi(vertex_id);
        send_to_all_neighbors(send_value);
    }
    else{
        VoteToHalt();
    }
    return;
}

extern "C" void* create_v_t() {
    return new PageRankVertex();
}

extern "C" void destroy_v_t(void* p) {
    App_PageRank* p_ = (App_PageRank*)p;
    delete p_;
}

extern "C" void* create_a_t() {
    return new App_PageRank();
}

extern "C" void destroy_a_t(void* p) {
    PageRankVertex* p_ = (PageRankVertex*)p;
    delete p_;
}


























