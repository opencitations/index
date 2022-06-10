#include <iostream>
#include <filesystem>
#include <sstream>
#include <fstream>
#include <cstring>
#include <zip.h>
#include <getopt.h>
#include <time.h>
#include <thread>
#include <iterator>

#include "BooPHF.h"

#include "StringHasher.hpp"

using namespace std;

typedef boomphf::mphf<string, StringHasher> boophf_t;

struct lookup_info
{
    zip *input_archive;
    uint file_index;
    zip_stat_t fstat;
    vector<pair<uint, uint>> offsets;
    boophf_t moph;
};

void usage(const char *basename)
{
    cerr << "usage: " << basename << " [OPTION]\n";
    cerr << "  -h, --help\t\t"
            "Print this help and exit.\n"
            "Additional help text logged in console.\n"
            "  -i, --input=INPUT_FILE\t"
            "input file containing oci new-line-separated\n"
            "  -m, --moph=DIRNAME\t"
            "Path to the moph directory.\n"
            "  -o, --oci=DIRNAME\t"
            "Path to the OCIs directory.\n";
}

int main(int argc, char **argv)
{
    /* take only the last portion of the path */
    const char *basename = strrchr(argv[0], '/');
    basename = basename ? basename + 1 : argv[0];

    string input;
    string oci_dir;
    string moph_dir;

    int required_parameters = 3;
    struct option longopts[] = {
        {"help", no_argument, nullptr, 'h'},
        {"oci", required_argument, nullptr, 'o'},
        {"input", required_argument, nullptr, 'i'},
        {"moph", required_argument, nullptr, 'm'},
        {0, 0, 0, 0}};

    auto parameter_error = [basename](string message)
    {
        cerr << basename << ": " << message << endl;
        usage(basename);
        return EXIT_FAILURE;
    };

    // Parse the parameters
    int opt;
    while ((opt = getopt_long(argc, argv, "hi:m:o:", longopts, 0)) != -1)
    {
        switch (opt)
        {
        case 'i':
            input = optarg;
            if (!filesystem::exists(input))
                return parameter_error("The input parameter must be a valid file");
            required_parameters--;
            break;
        case 'o':
            oci_dir = optarg;
            if (!filesystem::is_directory(oci_dir))
                return parameter_error("The oci parameter must be a valid directory");
            required_parameters--;
            break;
        case 'm':
            moph_dir = optarg;
            if (!filesystem::is_directory(moph_dir))
                return parameter_error("The moph parameter must be a valid directory");
            required_parameters--;
            break;
        case 'h':
            usage(basename);
            return EXIT_SUCCESS;
        default:
            usage(basename);
            return EXIT_FAILURE;
        }
    }
    if (required_parameters)
        return parameter_error("The mandatory parameters have not been provided");

    // Get current time
    struct timeval timet;
    double t_begin, t_end;
    gettimeofday(&timet, NULL);
    t_begin = timet.tv_sec + (timet.tv_usec / 1000000.0);

    filesystem::path file_path;
    filesystem::path input_directory(oci_dir);

    ifstream oci_input_file(input);
    vector<string> oci_list;
    string line;
    while (getline(oci_input_file, line))
    {
        oci_list.push_back(line);
    }
    oci_input_file.close();

    int err;
    char *archive_path;
    zip *input_archive;
    char errstr[1024];

    uint max_size = 0;

    cout << "Reading moph" << endl;

    zip_source *source;
    vector<lookup_info> lookup;
    for (const auto &entry : filesystem::directory_iterator(input_directory))
    {
        file_path = entry.path();

        // Process only zip files
        if (filesystem::path{file_path}.extension() == ".zip")
        {
            // Open zip archive
            input_archive = zip_open(file_path.c_str(), 0, &err);
            if (input_archive == NULL)
            {
                zip_error_to_str(errstr, sizeof(errstr), err, errno);
                cerr << "Cannot open zip archive " << file_path << endl;
                cerr << errstr << endl;
                return EXIT_FAILURE;
            }

            // Iterate over all the files in the zip archive
            for (uint i = 0; i < zip_get_num_files(input_archive); i++)
            {
                lookup_info file_info;
                if (zip_stat_index(input_archive, i, 0, &file_info.fstat) == 0)
                {
                    uint size = file_info.fstat.size;
                    if (size > max_size)
                        max_size = size;
                    string archive_name = file_path.filename().string();
                    string moph_filename = (filesystem::path(moph_dir) / filesystem::path(archive_name.substr(0, archive_name.length() - 4) + "_" + to_string(i))).string();

                    // Read offset pairs
                    ifstream offset_fin(moph_filename + ".csv");
                    string line;
                    while (getline(offset_fin, line))
                    {
                        pair<uint, uint> offset;
                        int delimiter = line.find(",");
                        offset.first = stoi(line.substr(0, delimiter));
                        offset.second = stoi(line.substr(delimiter + 1));
                        file_info.offsets.push_back(offset);
                    }

                    // Read moph
                    ifstream moph_fin(moph_filename + ".bin", ios::in | ios::binary);
                    file_info.moph.load(moph_fin);
                    file_info.file_index = i;
                    file_info.input_archive = input_archive;

                    lookup.push_back(file_info);
                }
            }
        }
    }
    int k = 0;
    vector<bool> results;
    char *buffer = (char *)calloc(max_size, sizeof(char));
    for (lookup_info info : lookup)
    {
        int j = 0;

        // Read the zipped file
        zip_file *file = zip_fopen_index(info.input_archive, info.file_index, 0);
        zip_fread(file, buffer, info.fstat.size);
        zip_fclose(file);

        for (string oci : oci_list)
        {
            uint i = info.moph.lookup(oci);
            bool result = false;
            if (i != ULLONG_MAX && i < info.offsets.size())
            {
                pair<uint, uint> offset = info.offsets[i];

                // Seek and read the oci position in the file
                char *real_oci = (char *)calloc(offset.second, sizeof(char));
                strncpy(real_oci, buffer + offset.first - 1, offset.second);

                cout << real_oci << endl;
                cout << oci << endl;

                // Compute the local result w.r.t current lookup info
                result = strcmp(oci.c_str(), real_oci) == 0;
            }
            // Update result
            if (j < results.size())
                results[j] = results[j] || result;
            else
                results.push_back(result);
            j += 1;
        }
    }

    // Print result
    for (int i = 0; i < results.size(); i++)
    {
        if (i != 0)
        {
            cout << ",";
        }
        cout << results[i] ? "1" : "0";
    }
    cout << endl;
    cout << results.size() << endl;
    gettimeofday(&timet, NULL);
    t_end = timet.tv_sec + (timet.tv_usec / 1000000.0);
    double elapsed = t_end - t_begin;
    cout << "The process of building the tables took " << elapsed / 60 << " minutes" << endl;
    return EXIT_SUCCESS;
}