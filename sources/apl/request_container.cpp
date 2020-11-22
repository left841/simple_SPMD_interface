#include "apl/request_container.h"

namespace apl
{

    request_block::request_block()
    { }

    request_block::~request_block()
    { assert(req_vec.empty()); }

    void request_block::store(MPI_Request req)
    { req_vec.push_back(req); }

    void request_block::store(request_block& req)
    {
        std::copy(req.req_vec.begin(), req.req_vec.end(), std::back_inserter(req_vec));
        req.req_vec.clear();
    }

    bool request_block::test_all()
    {
        int flag = true;
        apl_MPI_CHECKER(MPI_Testall(static_cast<int>(req_vec.size()), req_vec.data(), &flag, MPI_STATUSES_IGNORE));
        return flag;
    }

    void request_block::wait_all()
    {
        if (!req_vec.empty())
        {
            apl_MPI_CHECKER(MPI_Waitall(static_cast<int>(req_vec.size()), req_vec.data(), MPI_STATUSES_IGNORE));
            req_vec.clear();
        }
    }

    void request_block::cancel_all()
    {
        for (MPI_Request& req: req_vec)
            apl_MPI_CHECKER(MPI_Cancel(&req));
    }

    void request_block::free_all()
    {
        for (MPI_Request& req: req_vec)
            apl_MPI_CHECKER(MPI_Request_free(&req));
        req_vec.clear();
    }

    size_t request_block::size() const
    { return req_vec.size(); }

    MPI_Request* request_block::data()
    { return req_vec.data(); }

    const MPI_Request* request_block::data() const
    { return req_vec.data(); }

    void request_block::clear()
    { req_vec.clear(); }


    request_container::request_container()
    { }

    request_container::~request_container()
    { wait_all(); }

    void request_container::store(MPI_Request req)
    {
        req_vec.push_back(req);
        block_pos.push_back(1);
    }

    void request_container::store(request_block& req)
    {
        std::copy(req.data(), req.data() + req.size(), std::back_inserter(req_vec));
        block_pos.push_back(req.size());
        req.clear();
    }

    bool request_container::test_one()
    {
        int flag = true;
        apl_MPI_CHECKER(MPI_Testall(static_cast<int>(block_pos.back()), req_vec.data() + req_vec.size() - block_pos.back(), &flag, MPI_STATUSES_IGNORE));
        return flag;
    }

    void request_container::wait_one()
    {
        apl_MPI_CHECKER(MPI_Waitall(static_cast<int>(block_pos.back()), req_vec.data() + req_vec.size() - block_pos.back(), MPI_STATUSES_IGNORE));
        req_vec.resize(req_vec.size() - block_pos.back());
        block_pos.pop_back();
    }

    void request_container::cancel_one()
    {
        for (auto it = req_vec.end() - block_pos.back(); it != req_vec.end(); ++it)
            apl_MPI_CHECKER(MPI_Cancel(&(*it)));
    }

    void request_container::free_one()
    {
        for (auto it = req_vec.end() - block_pos.back(); it != req_vec.end(); ++it)
            apl_MPI_CHECKER(MPI_Request_free(&(*it)));
        req_vec.resize(req_vec.size() - block_pos.back());
        block_pos.pop_back();
    }

    bool request_container::test_all()
    {
        int flag = true;
        apl_MPI_CHECKER(MPI_Testall(static_cast<int>(req_vec.size()), req_vec.data(), &flag, MPI_STATUSES_IGNORE));
        return flag;
    }

    void request_container::wait_all()
    {
        apl_MPI_CHECKER(MPI_Waitall(static_cast<int>(req_vec.size()), req_vec.data(), MPI_STATUSES_IGNORE));
        req_vec.clear();
        block_pos.clear();
    }

    void request_container::cancel_all()
    {
        for (MPI_Request& req: req_vec)
            apl_MPI_CHECKER(MPI_Cancel(&req));
    }

    void request_container::free_all()
    {
        for (MPI_Request& req: req_vec)
            apl_MPI_CHECKER(MPI_Request_free(&req));
        req_vec.clear();
        block_pos.clear();
    }

}
