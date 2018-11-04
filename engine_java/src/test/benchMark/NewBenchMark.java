package benchMark;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class NewBenchMark {

    private final static String DB_PATH = "/Users/shaw/shawdb";  //数据库目录
    private EngineRace engineRace = new EngineRace();


    @Benchmark
    public void WriteTest() {
        try {
            engineRace.open(DB_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        engineRace.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(NewBenchMark.class.getSimpleName())
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(5)
                .build();

        new Runner(opt).run();
    }
}
