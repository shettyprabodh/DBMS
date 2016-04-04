#include <bits/stdc++.h>

using namespace std;

int rec_sz_1, rec_sz_2, page_size, no_of_pages, max_no_of_rounds,no_of_buckets;
int round_prime;
int size_rel_1, size_rel_2;


int number_of_records(char *file)
{
    int line_no;
    ifstream fileptr;
    string line;

    fileptr.open(file);
    line_no = 0;

    while(getline(fileptr,line))
    {
    if(line.compare("=================================================") == 0)
        continue;

    if(line.compare("#_of_pages") == 0)
        break;

    line_no++;
    }
    return line_no;
}

int hash_func(int x)
{
    int result;
    result = (x*(round_prime+1))%no_of_buckets ;
    return result;
}

string filename_gen(string file, int bucket_no)
{
    string result;
    result = file + ".round" + to_string(round_prime) + ".bucket" + to_string(bucket_no) + ".txt";
    return result;
}

void partition(char *file, int rec_size)
{
    int no_of_records, input_buffer, line_no;
    string input_line;
    ifstream fileptr1 (file);
    fileptr1.open(file);

    no_of_records = page_size/rec_size;

    vector < vector < int > > bucket(no_of_buckets);
    vector < int > no_pages_bucket(no_of_buckets,0);

    cout << "Hashing Round " << round_prime << "\n";
    cout << "Reading " << file << "\n";

    for(int i=0; i < no_of_buckets; i++ )
    {
        ofstream new_file (filename_gen(string(file), i), ios::app);
        new_file.open(filename_gen(string(file), i), ios::app );
        new_file.close();
    }

    line_no = 0;

    fileptr1.close();
    fileptr1.open(file);

    if(fileptr1.is_open())
    {
        cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

        while(getline(fileptr1,input_line))
        {
            line_no++;
            if(input_line.compare("=================================================") == 0)
                continue;

            if(input_line.compare("#_of_pages") == 0)
                break;
            input_buffer = stoi(input_line);
            int hashed_value = hash_func(input_buffer);

            if( no_pages_bucket[hashed_value] == 0 )
                no_pages_bucket[hashed_value]++;

            if(bucket[ hashed_value ].size() >= no_of_records )
            {
                ofstream flushing_file;
                flushing_file.open(filename_gen(string(file), hashed_value), ios::app );
                for(int j=0; j<no_of_records;j++ )
                {
                    int a = bucket[ hashed_value ].back();
                    bucket[ hashed_value ].pop_back();
                    flushing_file << a << "\n";
                }
                flushing_file << "=================================================\n";
                cout << "Page for bucket " << hash_func(input_buffer) << " full. Flushed to secondary storage.\n";
                flushing_file.close();
                bucket[ hashed_value ].push_back(input_buffer);
                cout << "Tuple: " << line_no << ": " << input_buffer << " Mapped to bucket: " << hashed_value << "\n";
                no_pages_bucket[ hashed_value ]++;
            }
            else
            {
                bucket[ hashed_value ].push_back(input_buffer);
                cout << "Tuple: " << line_no << ": " << input_buffer << " Mapped to bucket: " << hashed_value << "\n";
            }


        }
    }

    for(int i=0; i<no_of_buckets; i++)
    {
        if( !bucket[i].empty() )
        {
            ofstream flushing_file;
            flushing_file.open(filename_gen(string(file), i), ios::app);

            while( !bucket[i].empty() )
            {
                int a = bucket[i].back();
                bucket[i].pop_back();
                flushing_file << a << "\n";
            }
            flushing_file.close();
        }
    }

    for(int i=0; i<no_of_buckets ; i++)
    {
        ofstream flushing_file;
        flushing_file.open(filename_gen(string(file), i), ios::app);
        //flushing_file << "+++++++++++++++++++++++++++++++++++++++++++++++++\n";
        flushing_file << "#_of_pages\n";
        flushing_file << no_pages_bucket[i] << "\n";
        cout << filename_gen(string(file), i) << " : " << no_pages_bucket[i] << " pages.\n" ;
    }

    cout << "Done with " << file << ".\n\n\n";
}

void join_anyway(char *file1, char *file2)
{
    cout << "No of rounds execeded. Using nested block join.\n";

    string line1, line2;
    ifstream fileptr1, fileptr2;
    ofstream resultant_table;

    fileptr1.open(file1);
    resultant_table.open("resultant_table.txt", ios::app);
    cout << "Matching pairs are\n";

    while(getline(fileptr1,line1))
    {
        if(line1.compare("=================================================") == 0)
            continue;
        if(line1.compare("#_of_pages") == 0)
        {
            cout << "Bucket: " << ".No further processing required from " << file1 << "\n";
            break;
        }

        fileptr2.open(file2);
        while(getline(fileptr2,line2))
        {
            if(line2.compare("=================================================") == 0)
                continue;
            if(line2.compare("#_of_pages") == 0)
            {
                // cout << "Bucket: " << i << ".No further processing required.";
                break;
            }


            if(line1.compare(line2) == 0)
            {
                cout << line1 << " : " << line2 << "\n";
                resultant_table << line1 << "\n";
            }
        }
        fileptr2.close();
    }

    resultant_table.close();
    fileptr1.close();
    // if(round_prime>0)
    //     round_prime--;
    cout << "\n\n";

}

void join(char *file1, char *file2)
{
    int no_pages_bucket_1, no_pages_bucket_2;
    string line1, line2;
    string file_nxt_round_1, file_nxt_round_2;
    ifstream fileptr1, fileptr2;
    ofstream resultant_table;

    for(int i=0; i<no_of_buckets ; i++)
    {
        fileptr1.open(filename_gen(string(file1),i));
        fileptr2.open(filename_gen(string(file2),i));
        while(getline(fileptr1,line1))
        {
            if(line1.compare("#_of_pages") == 0)
            {
                getline(fileptr1,line1);
                no_pages_bucket_1 = stoi(line1);
            }
        }
        while(getline(fileptr2,line2))
        {
            if(line2.compare("#_of_pages") == 0)
            {
                getline(fileptr2,line2);
                no_pages_bucket_2 = stoi(line2);
            }
        }

        fileptr1.close();
        fileptr2.close();

        cout << "Bucket " << i << ": Total size is " << (no_pages_bucket_1 + no_pages_bucket_2) << " pages. \n";

        if(no_pages_bucket_1 + no_pages_bucket_2 >= no_of_pages)
        {
            cout << "Cannot perform in memory join.\n";

            if(round_prime >= max_no_of_rounds)
            {
                file_nxt_round_1 = filename_gen(string(file1),i);
                file_nxt_round_2 = filename_gen(string(file2),i);

                join_anyway((char *)file_nxt_round_1.c_str(), (char *)file_nxt_round_2.c_str());

            }
            else
            {
                int records_1, records_2;

                records_1 = number_of_records(file1);
                records_2 = number_of_records(file2);

                if((records_1*size_rel_1)/page_size == 0)
                    size_rel_1 = (records_1*rec_sz_1)/page_size;
                else
                    size_rel_1 = (records_1*rec_sz_1)/page_size + 1;

                if((records_2*size_rel_2)/page_size == 0)
                    size_rel_2 = (records_2*rec_sz_2)/page_size;
                else
                    size_rel_2 = (records_2*rec_sz_2)/page_size + 1;

                cout << "Performing next round of hashing for bucket " << i << "\n";
                cout << "Size of relation 1: " << size_rel_1 << "\n";
                cout << "Size of relation 2: " << size_rel_2 << "\n";
                cout << "Total number of available pages: " << no_of_pages << "\n";
                cout << "Number of buckets in hash table: " << no_of_pages - 1 << "\n\n\n";

                file_nxt_round_1 = filename_gen(string(file1),i);
                file_nxt_round_2 = filename_gen(string(file2),i);
                round_prime = round_prime + 1;

                partition((char *)file_nxt_round_1.c_str(),rec_sz_1);
                partition((char *)file_nxt_round_2.c_str(),rec_sz_2);
                join((char *)file_nxt_round_1.c_str(), (char *)file_nxt_round_2.c_str());
                if(round_prime>0)
                    round_prime--;
            }
        }
        else
        {
            //cout << "Bucket " << i << ": Total size is " << (no_pages_bucket_1 + no_pages_bucket_2) << " pages\n";
            cout << "Total available pages " << no_of_pages << "\n";
            cout << "Performing in memory join\n";
            cout << "Matching pairs are\n";
            fileptr1.open(filename_gen(string(file1),i));
            resultant_table.open("resultant_table.txt", ios::app);


            while(getline(fileptr1,line1))
            {
                if(line1.compare("=================================================") == 0)
                    continue;
                if(line1.compare("#_of_pages") == 0)
                {
                    cout << "Bucket: " << i << ". No further processing required from " << file1 << "\n";
                    break;
                }

                fileptr2.open(filename_gen(string(file2),i));
                while(getline(fileptr2,line2))
                {
                    if(line2.compare("=================================================") == 0)
                        continue;
                    if(line2.compare("#_of_pages") == 0)
                    {
                        // cout << "Bucket: " << i << ".No further processing required.";
                        break;
                    }

                    if(line1.compare(line2) == 0)
                    {
                        cout << line1 << " : " << line2 << "\n";
                        resultant_table << line1 << "\n";
                    }
                }
                fileptr2.close();
            }

            fileptr1.close();
            resultant_table.close();
            // if(round_prime>0)
            // {
            //     round_prime--;
            // }
            cout << "\n\n\n";
        }
    }
}


int main(int argc, char *argv[])
{
    char *file_1, *file_2;
    int records_1, records_2;
    string line1, line2;
    char *line_1, *line_2;
    line_1 = (char *)malloc(100*sizeof(char));
    line_2 = (char *)malloc(100*sizeof(char));

    //cout << argc << "\n";
    // for(int i=0; i < argc; i++)
    // {
    //     cout << argv[i] << "\n";
    // }

    file_1 = argv[1];
    file_2 = argv[2];
    rec_sz_1 = stoi(argv[3]);
    rec_sz_2 = stoi(argv[4]);
    page_size = stoi(argv[5]);
    no_of_pages = stoi(argv[6]);
    max_no_of_rounds = stoi(argv[7]);
    round_prime = 0;
    no_of_buckets = no_of_pages - 1;

    records_1 = number_of_records(file_1);
    records_2 = number_of_records(file_2);

    if((records_1*size_rel_1)/page_size == 0)
        size_rel_1 = (records_1*rec_sz_1)/page_size;
    else
        size_rel_1 = (records_1*rec_sz_1)/page_size + 1;

    if((records_2*size_rel_2)/page_size == 0)
        size_rel_2 = (records_2*rec_sz_2)/page_size;
    else
        size_rel_2 = (records_2*rec_sz_2)/page_size + 1;


    cout << "Size of relation 1: " << size_rel_1 << "\n";
    cout << "Size of relation 2: " << size_rel_2 << "\n";
    cout << "Total number of available pages: " << no_of_pages << "\n";
    cout << "Number of buckets in hash table: " << no_of_pages - 1 << "\n";
    cout << "\n\n";

    partition(file_1,rec_sz_1);
    partition(file_2,rec_sz_2);

    join(file_1,file_2);

    return 0;
}
