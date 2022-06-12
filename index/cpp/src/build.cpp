#include <iostream>
#include <experimental/filesystem>
#include <sstream>
#include <fstream>
#include <cstring>
#include <zip.h>
#include <getopt.h>
#include <time.h>
#include <thread>
#include <algorithm>
#include <iterator>

#include "BooPHF.h"

#include "StringHasher.hpp"

using namespace std;

void usage(const char *basename)
{
    cerr << "usage: " << basename << " [OPTION]\n";
    cerr << "  -h, --help\t\t"
            "Print this help and exit.\n"
            "  -w, --workers\t\t"
            "Number of workers to use to build the moph.\n"
            "  -v, --verbose\t\t"
            "Additional help text logged in console.\n"
            "  -i, --input=DIRNAME\t"
            "Path to the input directory.\n"
            "  -o, --output=DIRNAME\t"
            "Path to the output directory.\n"
            "  -b, --batchsize=batchsize\t"
            "Batch size to use to create hash tables, by default is 5E7.\n";
}

typedef boomphf::mphf<string, StringHasher> boophf_t;

void save_moph(vector<string> input_keys, vector<pair<uint, uint>> input_keys_offsets, bool verbose, uint workers, string filename)
{
    boophf_t *bphf = NULL;
    double t_begin, t_end;
    struct timeval timet;

    uint nelem = input_keys.size();

    if (verbose)
    {
        cout << "Construct MOPH with " << nelem << " elements" << endl;
    }

    gettimeofday(&timet, NULL);
    t_begin = timet.tv_sec + (timet.tv_usec / 1000000.0);

    // lowest bit/elem is achieved with gamma=1, higher values lead to larger mphf but faster construction/query
    // gamma = 2 is a good tradeoff (leads to approx 3.7 bits/key )
    double gammaFactor = 2.0;

    // build the mphf
    bphf = new boomphf::mphf<string, StringHasher>(nelem, input_keys, workers, gammaFactor);

    gettimeofday(&timet, NULL);
    t_end = timet.tv_sec + (timet.tv_usec / 1000000.0);
    double elapsed = t_end - t_begin;

    if (verbose)
    {
        cout << "MOPH constructed in " << elapsed << " seconds" << endl;
        cout << "MOPH bits per element: " << (float)(bphf->totalBitSize()) / nelem << endl;
        cout << "Saving the MOPH " << filename + ".bin"
             << "..." << endl;
    }
    ofstream moph_os(filename + ".bin", ios::out | ios::binary);
    bphf->save(moph_os);
    if (verbose)
    {
        cout << "MOPH saved on disk" << endl;
    }

    if (verbose)
    {
        cout << "Saving indexed offset " << filename + ".csv"
             << "..." << endl;
    }
    // Save offset vector in csv format in according to lookup table ranking
    ofstream offset_os(filename + ".csv", ios::out | ios::binary);
    vector<pair<uint, uint>> input_keys_offsets_ordered(input_keys_offsets.size());
    for (uint i = 0; i < input_keys_offsets.size(); i++)
    {
        uint position = bphf->lookup(input_keys[i]);
        input_keys_offsets_ordered[position] = input_keys_offsets[i];
    }
    for (uint i = 0; i < input_keys_offsets.size(); i++)
    {
        pair<uint, uint> offset = input_keys_offsets_ordered[i];
        offset_os << offset.first << "," << offset.second << endl;
    }
    offset_os.close();
    if (verbose)
    {
        cout << "Indexed offset saved" << endl;
    }
}

int main(int argc, char **argv)
{
    /* take only the last portion of the path */
    const char *basename = strrchr(argv[0], '/');
    basename = basename ? basename + 1 : argv[0];

    int verbose = 0;
    string input;
    string output;

    int required_parameters = 2;
    struct option longopts[] = {
        {"help", no_argument, nullptr, 'h'},
        {"verbose", no_argument, nullptr, 'v'},
        {"workers", required_argument, nullptr, 'w'},
        {"input", required_argument, nullptr, 'i'},
        {"output", required_argument, nullptr, 'o'},
        {"batchsize", required_argument, nullptr, 'b'},
        {0, 0, 0, 0}};

    auto parameter_error = [basename](string message)
    {
        cerr << basename << ": " << message << endl;
        usage(basename);
        return EXIT_FAILURE;
    };
    int workers = 1;
    int batch_size = 5E7;

    // Parse the parameters
    int opt;
    while ((opt = getopt_long(argc, argv, "hvi:w:o:b:", longopts, 0)) != -1)
    {
        switch (opt)
        {
        case 'w':
            workers = atoi(optarg);
            break;
        case 'v':
            verbose = 1;
            break;
        case 'i':
            input = optarg;
            if (!filesystem::is_directory(input))
                return parameter_error("The input must be a valid directory");
            required_parameters--;
            break;
        case 'b':
            batch_size = atoi(optarg);
            break;
        case 'o':
            output = optarg;
            if (!filesystem::exists(output))
            {
                filesystem::create_directories(output);
            }
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

    // Declare input variables
    filesystem::path input_directory(input);
    zip *input_archive;
    zip_stat_t f_stat;
    filesystem::path file_path;
    size_t file_length;
    zip_file *input_file;
    int err = 0;
    char errstr[1024];
    uint header = 0;

    // Iterate over the files in the input directory
    for (const auto &entry : filesystem::directory_iterator(input_directory))
    {
        file_path = entry.path();

        // Process only zip files
        if (filesystem::path{file_path}.extension() == ".zip")
        {
            // Open zip archive
            if (verbose)
                cout << "Processing : " << file_path << endl;
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
                if (zip_stat_index(input_archive, i, 0, &f_stat) == 0)
                {
                    // Open the zip file
                    input_file = zip_fopen_index(input_archive, i, 0);
                    if (input_file == NULL)
                    {
                        zip_error_to_str(errstr, sizeof(errstr), err, errno);
                        cerr << "Cannot open file " << f_stat.name << " in " << file_path << endl;
                        cerr << errstr << endl;
                        return EXIT_FAILURE;
                    }
                    if (verbose)
                    {
                        cout << "\t Working on : " << f_stat.name << endl;
                    }

                    char *buffer = (char *)calloc(f_stat.size, sizeof(char));
                    file_length = zip_fread(input_file, buffer, f_stat.size);
                    if (file_length < 0)
                    {
                        zip_error_to_str(errstr, sizeof(errstr), err, errno);
                        cerr << "Error filling buffer using file " << f_stat.name << " in " << file_path << endl;
                        cerr << errstr << endl;
                        return EXIT_FAILURE;
                    }

                    header = 1;
                    string lines(buffer);
                    istringstream split(lines);

                    // Oci list and their offset in the file
                    vector<string> input_keys;
                    vector<pair<uint, uint>> input_keys_offsets;
                    uint start = 0;

                    // Read all the oci and save the position in the file
                    for (string line; getline(split, line, '\n');)
                    {
                        if (header)
                        {
                            start += line.length() + 2;
                            header = 0;
                            continue;
                        }
                        uint end = line.find(',');
                        string oci = line.substr(0, end);
                        input_keys.push_back(oci);

                        pair<uint, uint> offset;
                        offset.first = start;
                        offset.second = oci.length();
                        input_keys_offsets.push_back(offset);
                        start += line.length() + 1;
                    }
                    string archive_name = file_path.filename().string();
                    save_moph(
                        input_keys,
                        input_keys_offsets,
                        verbose,
                        workers,
                        filesystem::path(output) / filesystem::path(archive_name.substr(0, archive_name.length() - 4) + "_" + to_string(i)));
                    free(buffer);
                }
            }
            zip_close(input_archive);
        }
    }

    gettimeofday(&timet, NULL);
    t_end = timet.tv_sec + (timet.tv_usec / 1000000.0);
    double elapsed = t_end - t_begin;
    cout << "The process of building the tables took " << elapsed / 60 << " minutes" << endl;
    return EXIT_SUCCESS;
}