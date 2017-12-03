#include "App.h"

//#include "Graph.h"

void App_Base::run_application_thread(int reponse_code){
    cout << "Inside run_application_thread: reponse_code: " << reponse_code <<"\n";
    if(graph_ptr == NULL){
        cout << "Inside run_application_thread: graph_ptr = NULL\n";
        return;
    }
    
    graph_ptr->get_file();
    cout <<"run_application_thread: Get file successfully\n";
    graph_ptr->start_worker_listening_from_master_thread();      //listen msg from master
    cout <<"run_application_thread: start_worker_listening_from_master_thread successfully\n";

    graph_ptr->start_worker_listening_thread();                 //listen msg from other workers
    cout <<"run_application_thread: start start_worker_listening_thread successfully\n";

    string str("");
    if(reponse_code == 1 || reponse_code == 0){
        str += "WS";
        str += to_string(reponse_code);
    }
    else{
        str += "WI";
    }
    
    if(my_vm_info.ip_addr_str != graph_ptr->get_master_ip()){
        int master_fd = tcp_open_connection(graph_ptr->get_master_ip(), MASTER_APP_PORT);
        if(master_fd == -1){            //maybe should not return. Should wait for new master
            cout << "Cannot send to master!! Something is wrong\n";
            return;
        }
        tcp_send_string(master_fd , str);
    }
    else{   //Send to itself
        if(str[1] == 'S'){
            master_scheduler_ptr->handle_WS_msg(-1,str);
        }
        else{
            master_scheduler_ptr->handle_WI_msg(-1, str);
        }
    }
    return;
}


//App_Base::start_building_graph(string file_name){
//    //Read and upload file
//
//}

void App_Base::set_file_name(string file_){
    file_name = file_;
}

string App_Base::get_file_name(){
    return file_name;
}

Graph_Base* App_Base::get_graph_ptr(){
    return graph_ptr;
}

void App_Base::set_graph_ptr(Graph_Base* graph_ptr_){
    graph_ptr  = graph_ptr_;
}


void App_Base::Output_function(map<string,string>& vertex_value_map){
    for(auto it = vertex_value_map.begin(); it != vertex_value_map.end(); it++){
        Add_to_Output_Container(it->first, it->second);
    }
}


void App_Base::Add_to_Output_Container(string vertex_id_str, string vertex_val){
    output_container[vertex_id_str] = vertex_val;
}

void App_Base::write_to_file(string file_name){
    FILE* fp = fopen(file_name.c_str(), "w");
//    string temp("Hello\n");
//    fwrite(temp.c_str(), sizeof(char), temp.size() , fp);
    
    for(auto it= output_container.begin(); it != output_container.end(); it++){
        string val_str("");
        val_str += it->first + " ";
        val_str += it->second + "\n";
        fwrite(val_str.c_str(), sizeof(char), val_str.size() , fp);
    }
    fclose(fp);
}

int App_Base::get_my_worker_id(){
    return graph_ptr->get_my_worker_id();
}


//auto it_start = GetVertexMapIterator_Start();
//auto it_end = GetVertexMapIterator_End();



//
//int App_Base::get_num_superstep(){
//    return num_superstep;
//}
//void App_Base::set_num_superstep(int num_){
//    num_superstep = num_;
//}
//
