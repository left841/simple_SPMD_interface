#include "apl/parallel_core.h"

namespace apl
{

    global_intracomm comm_world;
    global_intracomm comm_self;

    double parallel_engine::start_time;
    process parallel_engine::global_comm_rank = MPI_PROC_NULL;
    int parallel_engine::thread_support_level = MPI_THREAD_SINGLE;
    bool parallel_engine::library_initialized = false;
    bool parallel_engine::library_finalized = false;

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
        if (library_finalized)
            throw std::runtime_error("initializing library after finalizing is prohibited");
        if (!library_initialized)
        {
            int flag;
            apl_MPI_CHECKER(MPI_Initialized(&flag));
            if (!flag)
                apl_MPI_CHECKER(MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &thread_support_level));
            if (thread_support_level != MPI_THREAD_MULTIPLE)
                throw std::runtime_error("library need MPI_THREAD_MULTIPLE level of thread support");
            start_time = MPI_Wtime();
            comm_world.assign(MPI_COMM_WORLD);
            comm_self.assign(MPI_COMM_SELF);
            global_comm_rank = comm_world.rank();
            library_initialized = true;
        }
    }

    void parallel_engine::finalize_library()
    {
        if (!library_finalized)
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
            library_finalized = true;
            library_initialized = false;
        }
    }

    double parallel_engine::get_start_time()
    { return start_time; }

    int parallel_engine::get_thread_support_level()
    { return thread_support_level; }

    process parallel_engine::global_rank()
    { return global_comm_rank; }

}