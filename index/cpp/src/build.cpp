#include <iostream>
#include <filesystem>
#include <sstream>
#include <fstream>
#include <cstring>
#include <zip.h>
#include <getopt.h>
#include <time.h>
#include <thread>

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
            "Path to the input directory.\n";
}

int main(int argc, char **argv)
{
    /* take only the last portion of the path */
    const char *basename = strrchr(argv[0], '/');
    basename = basename ? basename + 1 : argv[0];

    int verbose = 0;
    string input;

    int required_parameters = 1;
    struct option longopts[] = {
        {"help", no_argument, nullptr, 'h'},
        {"verbose", no_argument, nullptr, 'v'},
        {"workers", required_argument, nullptr, 'w'},
        {"input", required_argument, nullptr, 'i'},
        {0, 0, 0, 0}};

    auto parameter_error = [basename](string message)
    {
        cerr << basename << ": " << message << endl;
        usage(basename);
        return EXIT_FAILURE;
    };
    int workers = 1;

    // Parse the parameters
    int opt;
    while ((opt = getopt_long(argc, argv, "hvi:w:", longopts, 0)) != -1)
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

    vector<string> input_keys;

    filesystem::path input_directory(input);
    zip_file *input_file;
    zip *input_archive;
    zip_stat_t f_stat;
    int err = 0;
    filesystem::path file_path;
    char errstr[1024];
    size_t file_length;
    int header = 0;
    vector<string> citations();
    int oci_size = 0;
    for (const auto &entry : filesystem::directory_iterator(input_directory))
    {
        file_path = entry.path();
        if (filesystem::path{file_path}.extension() == ".zip")
        {
            if (verbose)
            {
                cout << "Processing : " << file_path << endl;
            }
            input_archive = zip_open(file_path.c_str(), 0, &err);
            if (input_archive == NULL)
            {
                zip_error_to_str(errstr, sizeof(errstr), err, errno);
                cerr << "Cannot open zip archive " << file_path << endl;
                cerr << errstr << endl;
                return EXIT_FAILURE;
            }

            for (int i = 0; i < zip_get_num_files(input_archive); i++)
            {
                if (zip_stat_index(input_archive, i, 0, &f_stat) == 0)
                {
                    input_file = zip_fopen_index(input_archive, i, 0);
                    if (input_file == NULL)
                    {
                        zip_error_to_str(errstr, sizeof(errstr), err, errno);
                        cerr << "Cannot open file " << f_stat.name << " in " << file_path << endl;
                        cerr << errstr << endl;
                        return EXIT_FAILURE;
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
                    for (string line; getline(split, line, '\n');)
                    {
                        if (header)
                        {
                            header = 0;
                            continue;
                        }
                        string oci = line.substr(0, line.find(','));
                        input_keys.push_back(oci);
                    }
                    free(buffer);
                }
            }
            zip_close(input_archive);
        }
    }

    typedef boomphf::mphf<string, StringHasher> boophf_t;
    boophf_t *bphf = NULL;
    double t_begin, t_end;
    struct timeval timet;

    int nelem = input_keys.size();

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
        cout << "Saving the MOPH on disk..." << endl;
    }
    ofstream fout("moph.dat", ios::out | ios::binary);
    bphf->save(fout);
    if (verbose)
    {
        cout << "MOPH saved on disk" << endl;
    }
    return EXIT_SUCCESS;
}