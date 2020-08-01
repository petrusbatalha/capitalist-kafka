mod config_reader;

fn main() {
    let config = config_reader::read_config();
    println!("{:?}", config);
}
