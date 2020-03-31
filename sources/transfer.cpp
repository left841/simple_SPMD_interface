#include "transfer.h"

namespace auto_parallel
{

    sender::sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): comm(_comm), proc(_proc), q(_q)
    { tag = 0; }

    void sender::send(const void* buf, int size, MPI_Datatype type) const
    { MPI_Send(buf, size, type, proc, tag++, comm); }

    void sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    receiver::receiver(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): comm(_comm), proc(_proc), q(_q)
    { tag = 0; }

    void receiver::recv(void* buf, int size, MPI_Datatype type) const
    { MPI_Recv(buf, size, type, proc, tag++, comm, MPI_STATUS_IGNORE); }

    void receiver::irecv(void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    int receiver::probe(MPI_Datatype type) const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, type, &size);
        return size;
    }

    //send-recv specifications
    // char
    template<>
    void sender::send<char>(const char* buf, int size) const
    { MPI_Send(buf, size, MPI_CHAR, proc, tag++, comm); }

    template<>
    void sender::isend<char>(const char* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_CHAR, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<char>(char* buf, int size) const
    { MPI_Recv(buf, size, MPI_CHAR, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<char>(char* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_CHAR, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<char>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_CHAR, &size);
        return size;
    }

    // unsigned char
    template<>
    void sender::send<unsigned char>(const unsigned char* buf, int size) const
    { MPI_Send(buf, size, MPI_UNSIGNED_CHAR, proc, tag++, comm); }

    template<>
    void sender::isend<unsigned char>(const unsigned char* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_UNSIGNED_CHAR, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<unsigned char>(unsigned char* buf, int size) const
    { MPI_Recv(buf, size, MPI_UNSIGNED_CHAR, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<unsigned char>(unsigned char* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_UNSIGNED_CHAR, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<unsigned char>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &size);
        return size;
    }

    // short
    template<>
    void sender::send<short>(const short* buf, int size) const
    { MPI_Send(buf, size, MPI_SHORT, proc, tag++, comm); }

    template<>
    void sender::isend<short>(const short* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_SHORT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<short>(short* buf, int size) const
    { MPI_Recv(buf, size, MPI_SHORT, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<short>(short* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_SHORT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<short>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_SHORT, &size);
        return size;
    }

    // unsigned short
    template<>
    void sender::send<unsigned short>(const unsigned short* buf, int size) const
    { MPI_Send(buf, size, MPI_UNSIGNED_SHORT, proc, tag++, comm); }

    template<>
    void sender::isend<unsigned short>(const unsigned short* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_UNSIGNED_SHORT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<unsigned short>(unsigned short* buf, int size) const
    { MPI_Recv(buf, size, MPI_UNSIGNED_SHORT, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<unsigned short>(unsigned short* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_UNSIGNED_SHORT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<unsigned short>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_UNSIGNED_SHORT, &size);
        return size;
    }

    // int
    template<>
    void sender::send<int>(const int* buf, int size) const
    { MPI_Send(buf, size, MPI_INT, proc, tag++, comm); }

    template<>
    void sender::isend<int>(const int* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_INT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<int>(int* buf, int size) const
    { MPI_Recv(buf, size, MPI_INT, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<int>(int* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_INT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<int>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_INT, &size);
        return size;
    }

    // unsigned
    template<>
    void sender::send<unsigned>(const unsigned* buf, int size) const
    { MPI_Send(buf, size, MPI_UNSIGNED, proc, tag++, comm); }

    template<>
    void sender::isend<unsigned>(const unsigned* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_UNSIGNED, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<unsigned>(unsigned* buf, int size) const
    { MPI_Recv(buf, size, MPI_UNSIGNED, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<unsigned>(unsigned* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_UNSIGNED, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<unsigned>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_UNSIGNED, &size);
        return size;
    }

    // long
    template<>
    void sender::send<long>(const long* buf, int size) const
    { MPI_Send(buf, size, MPI_LONG, proc, tag++, comm); }

    template<>
    void sender::isend<long>(const long* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<long>(long* buf, int size) const
    { MPI_Recv(buf, size, MPI_LONG, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<long>(long* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<long>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_LONG, &size);
        return size;
    }

    // unsigned long
    template<>
    void sender::send<unsigned long>(const unsigned long* buf, int size) const
    { MPI_Send(buf, size, MPI_UNSIGNED_LONG, proc, tag++, comm); }

    template<>
    void sender::isend<unsigned long>(const unsigned long* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_UNSIGNED_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<unsigned long>(unsigned long* buf, int size) const
    { MPI_Recv(buf, size, MPI_UNSIGNED_LONG, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<unsigned long>(unsigned long* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_UNSIGNED_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<unsigned long>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_UNSIGNED_LONG, &size);
        return size;
    }

    // long long
    template<>
    void sender::send<long long>(const long long* buf, int size) const
    { MPI_Send(buf, size, MPI_LONG_LONG, proc, tag++, comm); }

    template<>
    void sender::isend<long long>(const long long* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_LONG_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<long long>(long long* buf, int size) const
    { MPI_Recv(buf, size, MPI_LONG_LONG, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<long long>(long long* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_LONG_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<long long>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_LONG_LONG, &size);
        return size;
    }

    // unsigned long long
    template<>
    void sender::send<unsigned long long>(const unsigned long long* buf, int size) const
    { MPI_Send(buf, size, MPI_UNSIGNED_LONG_LONG, proc, tag++, comm); }

    template<>
    void sender::isend<unsigned long long>(const unsigned long long* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_UNSIGNED_LONG_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<unsigned long long>(unsigned long long* buf, int size) const
    { MPI_Recv(buf, size, MPI_UNSIGNED_LONG_LONG, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<unsigned long long>(unsigned long long* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_UNSIGNED_LONG_LONG, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<unsigned long long>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_UNSIGNED_LONG_LONG, &size);
        return size;
    }

    // float
    template<>
    void sender::send<float>(const float* buf, int size) const
    { MPI_Send(buf, size, MPI_FLOAT, proc, tag++, comm); }

    template<>
    void sender::isend<float>(const float* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_FLOAT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<float>(float* buf, int size) const
    { MPI_Recv(buf, size, MPI_FLOAT, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<float>(float* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_FLOAT, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<float>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_FLOAT, &size);
        return size;
    }

    // double
    template<>
    void sender::send<double>(const double* buf, int size) const
    { MPI_Send(buf, size, MPI_DOUBLE, proc, tag++, comm); }

    template<>
    void sender::isend<double>(const double* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_DOUBLE, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<double>(double* buf, int size) const
    { MPI_Recv(buf, size, MPI_DOUBLE, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<double>(double* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_DOUBLE, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<double>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_DOUBLE, &size);
        return size;
    }

    // long double
    template<>
    void sender::send<long double>(const long double* buf, int size) const
    { MPI_Send(buf, size, MPI_LONG_DOUBLE, proc, tag++, comm); }

    template<>
    void sender::isend<long double>(const long double* buf, int size) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, MPI_LONG_DOUBLE, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    void receiver::recv<long double>(long double* buf, int size) const
    { MPI_Recv(buf, size, MPI_LONG_DOUBLE, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<>
    void receiver::irecv<long double>(long double* buf, int size) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, MPI_LONG_DOUBLE, proc, tag++, comm, &req);
        q->push(req);
    }

    template<>
    int receiver::probe<long double>() const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, MPI_LONG_DOUBLE, &size);
        return size;
    }

}
