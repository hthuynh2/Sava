#include "operations.h"
void ls_at_client(string file_name){
    bool isMaster = false;

    master_lock.lock();
    int cur_master_id = master_id;
    if(cur_master_id == my_vm_info.vm_num)
        isMaster = true;
    master_lock.unlock();

    string file_info("");

    //Create LS Request msg
    string msg = create_LS_msg(file_name);

    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            cout << "Master is currently failed. Please try again later!\n";
            return;
        }
        VM_info master_info =  vm_info_map[cur_master_id];
        membership_list_lock.unlock();

        //Send read request msg to S
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, MASTER_PORT);
        if(master_sock_fd == -1){
            cout << "Cannot make connection with master. Please try again later!\n";
            return ;
        }

        int numbytes = tcp_send_string(master_sock_fd, msg);
        if(numbytes != 0){
            cout << "Cannot make connection with master. Please try again later!\n";
            return ;
        }

        struct timeval timeout_tv;
        timeout_tv.tv_sec = READ_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        char buf[MAX_BUF_LEN];

        numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes < 0 ){
            cout << "TIMEOUT!\n";
            close(master_sock_fd);
            return;
        }
        string temp(buf,numbytes);
        file_info = temp;
        close(master_sock_fd);
    }
    else{
        file_info = handle_LS_msg(-1, msg, false);
    }
    if(file_info.size() < 6){
        cout << "File not exist.\n";
        return;
    }
    cout << "Inside LS: msg = " << file_info <<"\n";
    cout << "File " << file_name << " is stored at: \n";

    vector<int> reps(3);
    reps[0] = string_to_int(file_info.substr(0,2));
    reps[1] = string_to_int(file_info.substr(2,2));
    reps[2] = string_to_int(file_info.substr(4,2));
    membership_list_lock.lock();
    for(int i = 0 ; i < 3; i++){
        cout << ".......";
        if(membership_list.find(reps[i]) != membership_list.end())
            cout << "VM" <<reps[i] << " with ip address: "<< vm_info_map[reps[i]].ip_addr_str <<"\n";
    }
    membership_list_lock.unlock();
}


string create_LS_msg(string file_name){
    string msg("LS");
    msg += file_name;
    return msg;
}

string handle_LS_msg(int socket_fd, string msg, bool need_to_send){
    unsigned char ip_addr[4];
    string file_name = msg.substr(2);

    string ret;
    file_table_lock.lock();
    int count = 0;

    if(filename_map.find(file_name) == filename_map.end()){
        ret = "1";
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        file_table_lock.unlock();
        return ret;
    }
    else{
        set<int> rows = filename_map[file_name];
        membership_list_lock.lock();
        for(auto it = rows.begin(); it != rows.end(); it ++){
            if(membership_list.find(file_table[*it].replica) == membership_list.end()){
                break;
            }
            else{
                ret += int_to_string(file_table[*it].replica);
                count++;
            }
        }
        membership_list_lock.unlock();
    }
    file_table_lock.unlock();
    if(count != 3){
        ret = "1";
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        return ret;
    }
    else{
        if(need_to_send){
            tcp_send_string(socket_fd, ret);
            close(socket_fd);
        }
        return ret;
    }
    return "1";
}
