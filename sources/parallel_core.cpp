#include "parallel_core.h"

namespace auto_parallel
{

    double parallel_engine::start_time;
    size_t parallel_engine::object_count = 0;

    parallel_engine::parallel_engine(int* argc, char*** argv)
    {
        int flag;
        MPI_Initialized(&flag);
        if (!flag)
        {
            MPI_Init(argc, argv);
            start_time = MPI_Wtime();
        }
        ++object_count;
    }

    parallel_engine::~parallel_engine()
    {
        --object_count;
        if (object_count == 0)
            MPI_Finalize();
    }

    double parallel_engine::get_start_time()
    { return start_time; }

}