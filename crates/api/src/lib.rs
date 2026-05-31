//! Protocol-agnostic read API. `GraphqlApiServer` impls `domain::ApiServer`
//! over an `EventQueryRepository`.

mod graphql;
mod server;

pub use server::GraphqlApiServer;
