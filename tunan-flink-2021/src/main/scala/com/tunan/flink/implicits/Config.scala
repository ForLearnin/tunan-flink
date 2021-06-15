import com.tunan.flink.implicits.BaseConfig

object Config extends BaseConfig{

    configFileName  = "mysql.conf"


    def main(args: Array[String]): Unit = {

        println(config.DB)


    }

}
