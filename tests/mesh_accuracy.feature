Feature: Counters shared across replicas

  A rate limit is a property of the whole deployment, not of whichever replica happens to
  answer. Replicas therefore broadcast what they have consumed, and each deducts its peers'
  consumption from its own decisions.

  Sharing is not instantaneous, so the guarantee is bounded rather than exact: between two
  broadcasts a replica cannot know what its peers have just admitted. These scenarios pin
  both halves — what sharing achieves, and what it costs.

  Background:
    Given a rate limit of 30 requests per 10 seconds with a burst of 10

  Scenario: One replica admits exactly the burst
    Given 1 replica
    And counters are exchanged after every request
    When 20 requests for the same client arrive
    Then 10 requests are admitted

  Scenario: Two replicas sharing counters admit no more than one replica would
    Given 2 replicas
    And counters are exchanged after every request
    When 20 requests for the same client arrive
    Then 10 requests are admitted

  Scenario: Three replicas sharing counters admit no more than one replica would
    Given 3 replicas
    And counters are exchanged after every request
    When 30 requests for the same client arrive
    Then 10 requests are admitted

  Scenario: Replicas that never exchange counters each admit a full burst
    Given 2 replicas
    And counters are never exchanged
    When 20 requests for the same client arrive
    Then 20 requests are admitted

  Scenario: Overshoot between broadcasts stays within the interval bound
    Given 2 replicas
    And counters are exchanged after every 3 requests
    When 30 requests for the same client arrive
    Then at most 13 requests are admitted
    And at least 10 requests are admitted

  Scenario: A replica whose peers have gone silent counts alone
    Given 2 replicas
    And counters are exchanged after every request
    And the peers have been silent for 5 seconds
    When 20 requests for the same client arrive
    Then 20 requests are admitted
