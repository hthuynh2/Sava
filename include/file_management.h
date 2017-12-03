#ifndef file_management_h
#define file_management_h

#include "common.h"

class File_Manager{
public:
    void split_file(string orig_file, int num_file, string file_name);
    void merge_file(string output_file, vector<string> file_name_vector);
    void read_file(int socket_fd, int file_name);
    void write_file(int socket_fd, int file_name);

};

#endif



