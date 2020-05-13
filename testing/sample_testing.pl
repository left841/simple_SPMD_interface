# start
open $output_file, ">", "result_output.txt" or die;

@ARGV;
if (@ARGV < 1)
{
    print "usage (windows): sample_testing.pl <max_number_of_processes>\n";
    print "usage (linux): sample_testing.pl <max_number_of_processes> linux\n";
    exit;
}

$prefix = "";
$posfix = ".exe";
if (@ARGV > 1)
{
    if ($ARGV[1] eq "linux")
    {
        $prefix = "./";
        $posfix = "";
    }
}

for ($i = 1; $i < $ARGV[0]; $i *= 2)
{
    push @processes, $i;
}
push @processes, $ARGV[0];

$check_try = 5;

# matrix-matrix multiplication
#@matrix_mul_array = (1000, 2000, 5000, 10000);
@matrix_mul_array = (1000, 2000);

print $output_file "matrix-matrix_multiplication ";
print "matrix-matrix multiplication\n";

$execute_name = $prefix."matrix-matrix_multiplication".$posfix;

for ($i = 0; $i < @matrix_mul_array; ++$i)
{
    print $output_file $matrix_mul_array[$i]." ";
}
print $output_file "\n";

for ($i = 0; $i < @processes; ++$i)
{
    $proc_count = $processes[$i];
    print $output_file "${proc_count} ";
    for ($j = 0; $j < @matrix_mul_array; ++$j)
    {
        $size_param = $matrix_mul_array[$j];
        print "count: ${size_param}, processes: ${proc_count} start\n";
        $time = 0;
        for ($k = 0; $k < $check_try; ++$k)
        {
            $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param} ${size_param} ${size_param}`;
            # @arr = split(/\n/, $res);
            # if ($arr[0] ne "correct")
            # {
            #     print "error ".$arr[0]."\n";
            #     die;
            # }
            # $time += $arr[1];
            $time += $res;
        }
        $time /= $check_try;
        print $output_file "${time} ";
    }
    print $output_file "\n";
}
print $output_file "\n";

# matrix-vector multiplication
@matrix_vec_array = (5000, 10000, 20000);

print $output_file "matrix-vector_multiplication ";
print "matrix-vector multiplication\n";

$execute_name = $prefix."matrix-vector_multiplication".$posfix;

for ($i = 0; $i < @matrix_vec_array; ++$i)
{
    print $output_file $matrix_vec_array[$i]." ";
}
print $output_file "\n";

for ($i = 0; $i < @processes; ++$i)
{
    $proc_count = $processes[$i];
    print $output_file "${proc_count} ";
    for ($j = 0; $j < @matrix_vec_array; ++$j)
    {
        $size_param = $matrix_vec_array[$j];
        print "count: ${size_param}, processes: ${proc_count} start\n";
        $time = 0;
        for ($k = 0; $k < $check_try; ++$k)
        {
            $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param} ${size_param}`;
            $time += $res;
        }
        $time /= $check_try;
        print $output_file "${time} ";
    }
    print $output_file "\n";
}
print $output_file "\n";

# merge sort
@merge_sort_array = (10000000, 50000000, 100000000);

print $output_file "merge_sort ";
print "merge sort\n";

$execute_name = $prefix."merge_sort".$posfix;

for ($i = 0; $i < @merge_sort_array; ++$i)
{
    print $output_file $merge_sort_array[$i]." ";
}
print $output_file "\n";

for ($i = 0; $i < @processes; ++$i)
{
    $proc_count = $processes[$i];
    print $output_file "${proc_count} ";
    for ($j = 0; $j < @merge_sort_array; ++$j)
    {
        $size_param = $merge_sort_array[$j];
        print "count: ${size_param}, processes: ${proc_count} start\n";
        $time = 0;
        for ($k = 0; $k < $check_try; ++$k)
        {
            $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param}`;
            $time += $res;
        }
        $time /= $check_try;
        print $output_file "${time} ";
    }
    print $output_file "\n";
}
print $output_file "\n";

# dynamic merge sort
@dmerge_array = (10000000, 50000000, 100000000);

print $output_file "dynamic_merge_sort ";
print "dynamic merge sort\n";

$execute_name = $prefix."dynamic_merge_sort".$posfix;

for ($i = 0; $i < @dmerge_array; ++$i)
{
    print $output_file $dmerge_array[$i]." ";
}
print $output_file "\n";

for ($i = 0; $i < @processes; ++$i)
{
    $proc_count = $processes[$i];
    print $output_file "${proc_count} ";
    for ($j = 0; $j < @dmerge_array; ++$j)
    {
        $size_param = $dmerge_array[$j];
        print "count: ${size_param}, processes: ${proc_count} start\n";
        $time = 0;
        for ($k = 0; $k < $check_try; ++$k)
        {
            $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param}`;
            $time += $res;
        }
        $time /= $check_try;
        print $output_file "${time} ";
    }
    print $output_file "\n";
}
print $output_file "\n";

# quick sort
@quick_sort_array = (10000000, 50000000, 100000000);

print $output_file "quick_sort ";
print "quick sort\n";

$execute_name = $prefix."quick_sort".$posfix;

for ($i = 0; $i < @quick_sort_array; ++$i)
{
    print $output_file $quick_sort_array[$i]." ";
}
print $output_file "\n";

for ($i = 0; $i < @processes; ++$i)
{
    $proc_count = $processes[$i];
    print $output_file "${proc_count} ";
    for ($j = 0; $j < @quick_sort_array; ++$j)
    {
        $size_param = $quick_sort_array[$j];
        print "count: ${size_param}, processes: ${proc_count} start\n";
        $time = 0;
        for ($k = 0; $k < $check_try; ++$k)
        {
            $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param}`;
            $time += $res;
        }
        $time /= $check_try;
        print $output_file "${time} ";
    }
    print $output_file "\n";
}
print $output_file "\n";

# end
close $output_file;
