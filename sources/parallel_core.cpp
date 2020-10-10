#include "parallel_core.h"

namespace apl
{

    double parallel_engine::start_time;

    process parallel_engine::global_comm_rank = MPI_PROC_NULL;

    std::vector<MPI_Datatype> parallel_engine::created_datatypes;

    parallel_engine::parallel_engine(int* argc, char*** argv)
    {
        init_library(argc, argv);
    }

    parallel_engine::~parallel_engine()
    {
        finalize_library();
    }

    void parallel_engine::init_library(int* argc, char*** argv)
    {
        int flag;
        MPI_Initialized(&flag);
        if (!flag)
        {
            MPI_Init(argc, argv);
            start_time = MPI_Wtime();
            MPI_Comm_rank(MPI_COMM_WORLD, &global_comm_rank);
        }
    }

    void parallel_engine::finalize_library()
    {
        int flag;
        MPI_Initialized(&flag);
        if (flag)
        {
            for (MPI_Datatype& t: created_datatypes)
                MPI_Type_free(&t);
            MPI_Finalize();
        }
    }

    void parallel_engine::add_datatype(MPI_Datatype dt)
    {
        created_datatypes.push_back(dt);
    }

    double parallel_engine::get_start_time()
    { return start_time; }

    process parallel_engine::global_rank()
    { return global_comm_rank; }

}