#include "file_management.h"


void File_Manager::split_file(string orig_file, int num_file, string file_name){
    
    string count_line_cmd("wc -l ");
    count_line_cmd += orig_file;
    
    FILE* cmd_fp = popen(count_line_cmd.c_str(), "r");
    if(cmd_fp == NULL){
        cout << "Cannot count line! Sth is WRONG\n";
        return;
    }
    
    //Get number of lines
    char line[MAX_BUF_LEN];
    int total_line_num = 0;
    while (fgets(line, MAX_BUF_LEN, cmd_fp)) {
        int i = 0 ;
        while(!(line[i] <= '9' && line[i] >='0')){
            i++;
        }
        while(line[i] <= '9' && line[i] >='0'){
            total_line_num = total_line_num*10 + line[i] - '0';
            i++;
        }
        break;
    }
    fclose(cmd_fp);
    cmd_fp = NULL;
    
    cout << "File " << orig_file << " has "<< total_line_num << " lines\n";
    
    //Add empty line so that number of lines divisible by num_file
    int num_line_add = total_line_num % num_file;
    num_line_add = num_file - num_line_add;
    string new_str("");
    for(int i = 0 ; i < num_line_add; i++){
        new_str.push_back('\n');
    }
    
    std::ofstream f_stream;
    f_stream.open(orig_file.c_str(), std::ios_base::app);
    f_stream << new_str;
    f_stream.close();
    
    total_line_num += num_line_add;

    //Split file into string
    int line_per_file = total_line_num / num_file;
    string split_cmd = "split -l " + to_string(line_per_file) + " "  + orig_file + " " +file_name;
    cout<< split_cmd<<"\n";
    cmd_fp = popen(split_cmd.c_str(), "r");

    if(cmd_fp == NULL){
        cout << "Cannot count line! Sth is WRONG\n";
    }
    else{
        cout << "Done\n";
        fclose(cmd_fp);
    }
    return;
}


void File_Manager::merge_file(string output_file, vector<string> file_name_vector){
    
}


void File_Manager::read_file(int socket_fd, int file_name){
    
}

void File_Manager::write_file(int socket_fd, int file_name){
    
}





