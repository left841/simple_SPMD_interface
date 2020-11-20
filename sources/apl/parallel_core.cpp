#include "apl/parallel_core.h"

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
        apl_MPI_CHECKER(MPI_Initialized(&flag));
        if (!flag)
        {
            apl_MPI_CHECKER(MPI_Init(argc, argv));
            start_time = MPI_Wtime();
            apl_MPI_CHECKER(MPI_Comm_rank(MPI_COMM_WORLD, &global_comm_rank));
        }
    }

    void parallel_engine::finalize_library()
    {
        int flag;
        apl_MPI_CHECKER(MPI_Initialized(&flag));
        if (flag)
        {
            for (MPI_Datatype& t: created_datatypes)
                apl_MPI_CHECKER(MPI_Type_free(&t));
            apl_MPI_CHECKER(MPI_Finalize());
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