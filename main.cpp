#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sstream>
#include <fstream>
#include <bits/basic_file.h>
#include<fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
//#include <io.h>
// #define USERNAME_SIZE 32
// #define EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

using namespace std;

// Row structure
typedef struct
{
    uint32_t id;
    char username[32];
    char email[256];
} Row;

// Compact representation of row
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// Table structure
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

// Pager structure
typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    uint32_t num_rows;
    Pager *pager;
} Table;

typedef struct
{
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

// Get page from pager
void* get_page(Pager *pager, uint32_t page_num)
{
    if (page_num > TABLE_MAX_PAGES)
    {
        cout << "Tried to fetch page number out of bounds. " << page_num << " > " << TABLE_MAX_PAGES << endl;
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL)
    {
        // Cache miss. Allocate memory and load from file
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE)
        {
            num_pages += 1;
        }
        if (page_num <= num_pages)
        {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1)
            {
                cout << "Error reading file\n";
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

// where to read/write in memory for particular row
void *row_slot(Table *table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    //void *page = table->pages[page_num];
    //if (page == NULL)
    //{
        // Allocate memory only when we try to access page
        //page = table->pages[page_num] = malloc(PAGE_SIZE);
    //}

    void *page = get_page(table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    // return page + byte_offset;
    return static_cast<char *>(page) + byte_offset;
}


// code to convert to and from the compact representation.
void serialize_row(Row *source, void *destination)
{
    memcpy(static_cast<char *>(destination) + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(static_cast<char *>(destination) + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(static_cast<char *>(destination) + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination)
{
    memcpy(&(destination->id), static_cast<char *>(source) + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), static_cast<char *>(source) + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), static_cast<char *>(source) + EMAIL_OFFSET, EMAIL_SIZE);
}

void execute_insert(Statement *statement, Table *table)
{
    if (table->num_rows >= TABLE_MAX_ROWS)
    {
        cout << "Table is full\n";
        return;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
}

void execute_select(Statement *statement, Table *table)
{
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++)
    {
        deserialize_row(row_slot(table, i), &row);
        cout << "ID: " << row.id << " Username: " << row.username << " Email: " << row.email << endl;
    }
}


// open a pager
Pager *pager_open(const char *filename)
{
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        cout << "Unable to open file\n";
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager *pager = (Pager *)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
}

// create a new table
Table *new_table(const char *filename)
{
    Pager *pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;
    // for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    //{
    //     table->pages[i] = NULL;
    // }
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
}

// free memory allocated for table
/*void free_table(Table *table)
{
    for (int i = 0; table->pages[i]; i++)
    {
        free(table->pages[i]);
    }
    free(table);
}
*/

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    if (pager->pages[page_num] == NULL)
    {
        cout << "Tried to flush null page\n";
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1)
    {
        cout << "Error seeking: " << errno << endl;
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1)
    {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}

void db_close(Table *table)
{
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
    for (uint32_t i = 0; i < num_full_pages; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    // There may be a partial page to write to the end of the file
    // This should not be needed after the full implementation
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0 && pager->pages[num_full_pages] != NULL)
    {
        pager_flush(pager, num_full_pages, num_additional_rows * ROW_SIZE);
        free(pager->pages[num_full_pages]);
        pager->pages[num_full_pages] = NULL;
    }
    int result = close(pager->file_descriptor);
    if (result == -1)
    {
        cout << "Error closing db file.\n";
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}



int main(int argc, char *argv[])
{
    cout << "Welcome to AntonDB\n";
    if(argc < 2)
    {
        cout<<"Must supply a database filename\n";
        exit(EXIT_FAILURE);
    }
    const char *filename = argv[1];
    Table *table = new_table(filename);
    //Table *table = new_table();
    while (true)
    {
        cout << "AntonDB>";
        string input;
        getline(cin, input);
        istringstream iss(input);
        string command;
        Row row;
        Statement statement;
        iss >> command;
        if (command == "exit")
        {
            db_close(table);
            cout << "Exiting AntonDB\n";
            break;
        }
        else if (command == "help")
            cout << "exit: to exit the AntonDB\nhelp: to get help\n";
        else if (command == "select")
        {
            //  if(row.username == "")
            //     cout<<"Empty database\n";
            // cout<<"ID: "<<row.id<<" Username: "<<row.username<<" Email: "<<row.email<<endl;
            execute_select(&statement, table);
        }
        else if (command == "insert")
        {
            // iss >> num >> username >> email;
            // row.id = num;
            // strcpy(row.username, username.c_str());
            // strcpy(row.email, email.c_str());
            // cout<<"Inserted into the database\n";
            // cout<<num<<" "<<username<<" "<<email<<endl;
            if (!(iss >> statement.row_to_insert.id >> statement.row_to_insert.username >> statement.row_to_insert.email))
            {
                std::cerr << "Invalid input format." << std::endl;
                return 1;
            }
            execute_insert(&statement, table);
        }

        else
            cout << "Invalid input\n";
    }
    return 0;
}
