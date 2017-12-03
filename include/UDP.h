//
//  UDP.hpp
//
//
//  Created by Hieu Huynh on 10/7/17.
//

#ifndef UDP_hpp
#define UDP_hpp

#include <stdio.h>
#include "common.h"
#include "common1.h"
using namespace std;

typedef std::chrono::high_resolution_clock clk;
typedef std::chrono::time_point<clk> timepnt;
typedef std::chrono::milliseconds unit_milliseconds;
typedef std::chrono::microseconds unit_microseconds;
typedef std::chrono::nanoseconds unit_nanoseconds;
#define MAX_BUF_SIZE 1024

int tcp_open_connection(string dest_ip, string dest_port);
int tcp_send_string(int sockfd, string& str);
// int tcp_receive_short_msg(int sockfd, char* buf);
int tcp_bind_to_port(string port);

int tcp_send_string_with_size(int sockfd, string& str_);
string tcp_receive_str(int sockfd);

using namespace std;
class UDP{
private:
    char msg_buf[1024];
    int msg_buf_idx;
    queue<string> msg_q;    //Message queue

public:
    UDP();
    string read_msg_non_block(int time_out);
    string receive_msg();
    void getlines_(int fd);
    vector<string> buf_to_line(char* buf, int buf_size);
    void send_msg(string dest_addr, string msg);


};
#endif /* UDP_hpp */
