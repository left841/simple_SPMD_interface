#include "apl/parallel_core.h"

namespace apl
{

    global_intracomm comm_world;
    global_intracomm comm_self;

    double parallel_engine::start_time;

    process parallel_engine::global_comm_rank = MPI_PROC_NULL;

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
            comm_world.assign(MPI_COMM_WORLD);
            comm_self.assign(MPI_COMM_SELF);
            global_comm_rank = comm_world.rank();
        }
    }

    void parallel_engine::finalize_library()
    {
        int flag;
        apl_MPI_CHECKER(MPI_Initialized(&flag));
        if (flag)
        {
            for (MPI_Datatype& t: simple_datatype::created_datatypes)
                apl_MPI_CHECKER(MPI_Type_free(&t));
            comm_self.unassign();
            comm_world.unassign();
            apl_MPI_CHECKER(MPI_Finalize());
        }
    }

    double parallel_engine::get_start_time()
    { return start_time; }

    process parallel_engine::global_rank()
    { return global_comm_rank; }

}