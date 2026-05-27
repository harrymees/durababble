---
title: "Durababble"
weight: 1
---

# Durababble

> Durable Ruby for work that just doesn't fit in one job.

```ruby
class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = charge_card(order)
    label = buy_shipping_label(order, payment)

    { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
  end

  step retry: { maximum_attempts: 5, schedule: [1, 5, 30] }
  def charge_card(order)
    Payments.charge( order.fetch("card_token"), amount: order.fetch("total_cents") )
  end

  step def buy_shipping_label(order, payment)
    Shipping.buy_label(order.fetch("address"), payment_id: payment.fetch("id") )
  end
end

handle = FulfillOrder.start(order)
handle.result
```

Durababble is a Ruby durable execution library for workflows and durable objects that persist progress in your existing application database. Use it for work that might run for a long time and must survive process exits, retries, deploys, and changes in which process is running the code.

Durababble exists for the middle ground where background jobs are too coarse but running a separate workflow system like Temporal is too much. It keeps orchestration in ordinary Ruby while making the important boundaries explicitly transacted database state. For bonus points, you also get cheap intra-cluster RPC, which allows you to frequently communicate with durable objects and workflows without burdening you with the usual scheduling and service discovery necessary to do this.

Workflow orchestration can also call workflow and durable-object handles directly; those in-workflow handle calls are persisted as workflow history commands so replay and crash recovery do not send the outbound RPC twice.

New here? Jump to the [Quickstart](quickstart.md) for a tour of the features. Detailed guarantees live in [the spec](../spec.md) and [the architecture overview](internals/architecture.md).
