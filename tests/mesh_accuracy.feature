Feature: Counters shared across replicas

  A rate limit is a property of the whole deployment, not of whichever replica happens to
  answer. Replicas therefore tell each other what they have admitted, and each debits its
  peers' admissions from its own bucket as if the requests had arrived there.

  Sharing is not instantaneous, so the guarantee is bounded rather than exact: between two
  exchanges a replica cannot know what its peers have just admitted. These scenarios pin
  both halves — what sharing achieves, and what it costs — for a burst at one instant and
  for traffic sustained over a minute, where the limit is the rate rather than the burst.

  Background:
    Given a rate limit of 30 requests per 10 seconds with a burst of 10

  Scenario: One replica admits exactly the burst
    Given 1 replica
    And counters are exchanged after every request
    When 20 requests for the same client arrive at once
    Then 10 requests are admitted

  Scenario: Two replicas sharing counters admit no more than one replica would
    Given 2 replicas
    And counters are exchanged after every request
    When 20 requests for the same client arrive at once
    Then 10 requests are admitted

  Scenario: Three replicas sharing counters admit no more than one replica would
    Given 3 replicas
    And counters are exchanged after every request
    When 30 requests for the same client arrive at once
    Then 10 requests are admitted

  Scenario: Replicas that never exchange counters each admit a full burst
    Given 2 replicas
    And counters are never exchanged
    When 20 requests for the same client arrive at once
    Then 20 requests are admitted

  Scenario: Overshoot between exchanges stays within the exchange bound
    Given 2 replicas
    And counters are exchanged after every 3 requests
    When 30 requests for the same client arrive at once
    Then at most 13 requests are admitted
    And at least 10 requests are admitted

  Scenario: Sustained traffic spread over three replicas is admitted at the configured rate
    Given 3 replicas
    And counters are exchanged after every request
    When 20 requests per second for the same client arrive for 60 seconds
    Then at most 195 requests are admitted
    And at least 185 requests are admitted

  Scenario: Sustained traffic spread over replicas that never exchange is admitted at N times the rate
    Given 3 replicas
    And counters are never exchanged
    When 20 requests per second for the same client arrive for 60 seconds
    Then at least 560 requests are admitted

  Scenario: Sustained traffic at one replica is admitted at the configured rate
    Given 3 replicas
    And counters are exchanged after every request
    When 20 requests per second for the same client arrive for 60 seconds, all at one replica
    Then at most 195 requests are admitted
    And at least 185 requests are admitted

  Scenario: Sustained traffic that moves between replicas is admitted at the configured rate
    Given 3 replicas
    And counters are exchanged after every request
    When 20 requests per second for the same client arrive for 60 seconds, moving to the next replica every 10 seconds
    Then at most 195 requests are admitted
    And at least 185 requests are admitted
