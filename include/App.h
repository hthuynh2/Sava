#ifndef App_Base_h
#define App_Base_h
#include "Vertex.h"
#include "file_management.h"
#include "common.h"
#include "UDP.h"
#include "master_scheduler.h"
#define OUTPUT_FILE_NAME "graph_output"


class App_Base{
public:
    App_Base(){};
    virtual ~App_Base(){};
//    virtual void set_graph_ptr(map<int,int>& worker_map, int master_vm_num) = 0;
//    virtual void run() = 0;
    virtual void set_app_info() = 0;
    virtual void Output_function(map<string,string>& vertex_value_map);

    void run_application_thread(int cur_num);
    void set_file_name(string file_);
    string get_file_name();
//    int get_num_superstep();
//    void set_num_superstep(int num_);
    Graph_Base* get_graph_ptr();
    void Add_to_Output_Container(string vertex_id_str, string val);
    virtual void write_to_file(string file_name);
    void set_graph_ptr(Graph_Base* graph_ptr_);
    int get_my_worker_id();
    map<string,string> get_output_container(){
        return output_container;
    }

protected:
    Graph_Base* graph_ptr;
    string file_name;
    map<string,string> output_container;
    int64_t num_send;
    int64_t num_receive;
};

extern App_Base* app_ptr;


typedef void* create_a();       //Might be wrong??!!???
typedef void destroy_a(void*);


#endif

















