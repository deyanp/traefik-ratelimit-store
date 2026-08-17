mod steps;

#[tokio::main]
async fn main() {
    use cucumber::World as _;

    // Scenarios share no state — each builds its own replicas in a Given — so they run
    // concurrently by default.
    steps::World::cucumber()
        .with_default_cli()
        .run_and_exit("tests")
        .await;
}
