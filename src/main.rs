mod config_reader;

fn main() {
    let config = config_reader::read_config();
    println!("Config baby {:?}", config.0);
    println!("TOPICO {:?}", config.1);
}
