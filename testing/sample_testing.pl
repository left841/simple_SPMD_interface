# start
open $output_file, ">", "result_output.txt" or die;

@ARGV;
if (@ARGV < 1)
{
    print "usage (windows): sample_testing.pl -exe_vec number_of_setups <max_number_of_processes> <max_number_of_threads>\n";
    print "usage (linux): sample_testing.pl -exe_vec number_of_setups <max_number_of_processes> <max_number_of_threads> -linux\n";
    exit;
}

$prefix = "";
$postfix = ".exe";
$check_try = 5;
$check_work = 0;

$i = 0;
while ($i < @ARGV)
{
    if ($ARGV[$i] eq "-exe_vec")
    {
        $setups = $ARGV[$i + 1];
        for ($j = 0; $j < $setups; ++$j)
        {
            $i += 2;
            push @processes, $ARGV[$i];
            push @threads, $ARGV[$i + 1];
        }
        ++$i;
    }
    elsif (($ARGV[$i] eq "-t") or ($ARGV[$i] eq "-trys"))
    {
        $check_try = $ARGV[++$i];
    }
    elsif (($ARGV[$i] eq "-c") or ($ARGV[$i] eq "-check"))
    {
        $check_work = 1;
    }
    elsif ($ARGV[$i] eq "-linux")
    {
        $prefix = "./";
        $postfix = "";
    }
    elsif ($ARGV[$i] eq "-windows")
    {
        $prefix = "";
        $postfix = ".exe";
    }
    ++$i;
}

@execution_tasks =
(
    {
        'name' => "matrix-matrix multiplication",
        'exe_name' => "matrix-matrix_multiplication",
        'size_params' => ["500 500 500", "1000 1000 1000", "1500 1500 1500", "2000 2000 2000"],
        'other_params' => "",
        'check' => "false"
    },
    {
        'name' => "matrix-vector multiplication",
        'exe_name' => "matrix-vector_multiplication",
        'size_params' => ["5000 5000", "10000 10000", "20000 20000"],
        'other_params' => "",
        'check' => "false"
    },
    {
        'name' => "merge sort",
        'exe_name' => "merge_sort",
        'size_params' => ["10000000", "50000000", "100000000"],
        'other_params' => "",
        'check' => "false"
    },
    {
        'name' => "dynamic merge sort",
        'exe_name' => "dynamic_merge_sort",
        'size_params' => ["10000000", "50000000", "100000000"],
        'other_params' => "",
        'check' => "false"
    },
    {
        'name' => "quick sort",
        'exe_name' => "quick_sort",
        'size_params' => ["10000000", "50000000", "100000000"],
        'other_params' => "",
        'check' => "false"
    }
);

for ($exe_data = 0; $exe_data < @execution_tasks; ++$exe_data)
{
    print $output_file $execution_tasks[$exe_data]{"name"}."\n";
    print $output_file "processes;threads;";
    print $execution_tasks[$exe_data]{"name"}."\n";

    $execute_name = $prefix.$execution_tasks[$exe_data]{"exe_name"}.$postfix;

    $size_array = $execution_tasks[$exe_data]{"size_params"};

    for ($i = 0; $i < @{$size_array}; ++$i)
    {
        print $output_file ${$size_array}[$i].";";
    }
    print $output_file "\n";

    for ($i = 0; $i < @processes; ++$i)
    {
        $proc_count = $processes[$i];
        $thread_count = $threads[$i];
        print $output_file "${proc_count};${thread_count};";

        $other_params = $prefix.$execution_tasks[$exe_data]{"other_params"};
        for ($j = 0; $j < @{$size_array}; ++$j)
        {
            $size_param = ${$size_array}[$j];
            print "size: ${size_param}, processes: ${proc_count}, threads: ${thread_count} start\n";
            if (($execution_tasks[$exe_data]{"check"} eq "true") or $check_work)
            {
                $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param} ${other_params} -check`;
                @arr = split(/\n/, $res);
                if ($arr[0] ne "correct")
                {
                    print "error: ".$arr[0]."\n";
                }
                $out_tmp = $arr[0];
                print $output_file "${out_tmp};";
            }
            else
            {
                $all_time = 0;
                for ($k = 0; $k < $check_try; ++$k)
                {
                    $res = `mpiexec -n ${proc_count} ${execute_name} -s ${size_param} ${other_params}`;
                    $all_time += $res;
                }
                $all_time /= $check_try;
                print $output_file "${all_time};";
            }
        }
        print $output_file "\n";
    }
    print $output_file "\n";
}

close $output_file;
