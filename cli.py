#!/usr/bin/env python3
import csv
import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List

import click


@click.group()
@click.option("--debug/--no-debug", default=False, help="Debug output, or no debug output.")
@click.pass_context
def interface(ctx: Dict, debug: bool) -> None:
    """Ampla engineering takehome ledger calculator."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug  # you can use ctx.obj['DEBUG'] in other commands to log or print if DEBUG is on
    ctx.obj["DB_PATH"] = os.path.join(os.getcwd(), "db.sqlite3")
    if debug:
        click.echo(f"[Debug mode is on]")


@interface.command()
@click.pass_context
def create_db(ctx: Dict) -> None:
    """Initialize sqlite3 database."""
    if os.path.exists(ctx.obj["DB_PATH"]):
        click.echo("Database already exists")
        return

    with sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        if not connection:
            click.echo(
                "Error: Unable to create sqlite3 db file. Please ensure sqlite3 is installed on your system and "
                "available in PATH!"
            )
            return

        cursor = connection.cursor()
        cursor.execute(
            """
            create table events
            (
                id integer not null primary key autoincrement,
                type varchar(32) not null,
                amount decimal not null,
                date_created date not null
                CHECK (type IN ("advance", "payment"))
            );
        """
        )
        connection.commit()
    click.echo(f"Initialized database at {ctx.obj['DB_PATH']}")


@interface.command()
@click.pass_context
def drop_db(ctx: Dict) -> None:
    """Delete sqlite3 database."""
    if not os.path.exists(ctx.obj["DB_PATH"]):
        click.echo(f"SQLite database does not exist at {ctx.obj['DB_PATH']}")
    else:
        os.unlink(ctx.obj["DB_PATH"])
        click.echo(f"Deleted SQLite database at {ctx.obj['DB_PATH']}")


@interface.command()
@click.argument("filename", type=click.Path(exists=True, writable=False, readable=True))
@click.pass_context
def load(ctx: Dict, filename: str) -> None:
    """Load events with data from csv file."""
    if not os.path.exists(ctx.obj["DB_PATH"]):
        click.echo(f"Database does not exist at {ctx.obj['DB_PATH']}, please create it using `create-db` command")
        return

    loaded = 0
    with open(filename) as infile, sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        cursor = connection.cursor()
        reader = csv.reader(infile)
        for row in reader:
            cursor.execute(
                f"insert into events (type, amount, date_created) values (?, ?, ?)", (row[0], row[2], row[1])
            )
            loaded += 1
        connection.commit()

    click.echo(f"Loaded {loaded} events from {filename}")


@interface.command()
@click.argument("end_date", required=False, type=click.STRING)
@click.pass_context
def balances(ctx: Dict, end_date: str = None) -> None:
    """Display balance statistics as of `end_date`."""
    # NOTE: You may not change the function signature of `balances`,
    #       however you may implement it any way you want, so long
    #       as you adhere to the format specification.
    #       Here is some code to get you started!
    if end_date is None:
        end_date = datetime.now().date().isoformat()

    user_global_balance = UserGlobalBalance()

    # query events from database example
    with sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        cursor = connection.cursor()
        result = cursor.execute("select * from events order by date_created asc;")
        events = result.fetchall()

        for event in events:
            if datetime.strptime(end_date, '%Y-%m-%d') >= datetime.strptime(event[3], '%Y-%m-%d'):
                if event[1] == "advance":
                    user_global_balance.create_advance(Decimal(str(event[2])), event[3])
                elif event[1] == "payment":
                    user_global_balance.pay_advance(Decimal(str(event[2])), event[3])

    global_statement = user_global_balance.get_global_statement(end_date)

    overall_payments_for_future = global_statement["overall_payments_for_future"]
    overall_advance_balance = global_statement["overall_advance_balance"]
    overall_interest_payable_balance = global_statement["overall_interest_payable_balance"]
    overall_interest_paid = global_statement["overall_interest_paid"]
    individual_advance_statement = global_statement["individual_advance_statement"]

    click.echo("Advances:")
    click.echo("----------------------------------------------------------")
    # NOTE: This initial print adheres to the format spec.
    click.echo("{0:>10}{1:>11}{2:>17}{3:>20}".format("Identifier", "Date", "Initial Amt", "Current Balance"))

    for x in individual_advance_statement:
        click.echo("{0:>10}{1:>11}{2:>17.2f}{3:>20.2f}".format(
            x["identifier"],
            x["created_at"],
            x["initial_amount"],
            x["balance"]
        ))

    # print summary statistics
    # NOTE: These prints adhere to the format spec.
    click.echo("\nSummary Statistics:")
    click.echo("----------------------------------------------------------")
    click.echo("Aggregate Advance Balance: {0:31.2f}".format(overall_advance_balance))
    click.echo("Interest Payable Balance: {0:32.2f}".format(overall_interest_payable_balance))
    click.echo("Total Interest Paid: {0:37.2f}".format(overall_interest_paid))
    click.echo("Balance Applicable to Future Advances: {0:>19.2f}".format(overall_payments_for_future))


class UserGlobalBalance:
    def __init__(self):
        self.overall_payments_for_future = Decimal(0)
        self.actives_advances: List[Advance] = []

    def get_global_statement(self, end_date: str):
        individual_advance_statement = []

        date = datetime.strptime(end_date, '%Y-%m-%d')
        overall_advance_balance = Decimal("0")
        overall_interest_payable_balance = Decimal("0")
        overall_interest_paid = Decimal("0")
        for advance in self.actives_advances:
            advance_statement = advance.get_statement(date)
            overall_advance_balance += advance_statement["balance"]
            overall_interest_payable_balance += advance_statement["interest_payable_balance"]
            overall_interest_paid += advance_statement["interest_paid"]
            individual_advance_statement.append(advance_statement)

        return {
            "overall_payments_for_future": self.overall_payments_for_future,
            "overall_advance_balance": overall_advance_balance,
            "overall_interest_payable_balance": overall_interest_payable_balance,
            "overall_interest_paid": overall_interest_paid,
            "individual_advance_statement": individual_advance_statement
        }

    def create_advance(self, balance: Decimal, create_date):
        advance_date = datetime.strptime(create_date, '%Y-%m-%d')
        self.actives_advances.append(
            Advance(
                SimpleInterestRateStrategy(Decimal("0.00035")),
                len(self.actives_advances) + 1,
                balance,
                advance_date
            )
        )

        if self.overall_payments_for_future > Decimal("0"):
            amount = self.overall_payments_for_future
            self.overall_payments_for_future = Decimal("0")
            self.pay_advance(amount, create_date)

    def pay_advance(self, amount: Decimal, date: str):
        paid_date = datetime.strptime(date, '%Y-%m-%d')
        for advance in self.actives_advances:
            amount = advance.pay_interest(amount, paid_date)
            if amount == Decimal("0"):
                break
        if amount > Decimal("0"):
            for advance in self.actives_advances:
                amount = advance.pay(amount)
                if amount == Decimal("0"):
                    break
        if amount > Decimal("0"):
            self.overall_payments_for_future += amount


class InterestRateStrategyBase(ABC):
    def __init__(self, daily_interest):
        self.daily_interest = daily_interest

    @abstractmethod
    def calculate(self, amount: Decimal, days: Decimal) -> Decimal:
        return NotImplemented


class SimpleInterestRateStrategy(InterestRateStrategyBase):
    def calculate(self, amount: Decimal, days: Decimal) -> Decimal:
        return (amount * (Decimal("1") + self.daily_interest * days)) - amount


class Advance:
    def __init__(
        self,
        interest_rate_strategy: InterestRateStrategyBase,
        identifier,
        balance: Decimal,
        created_at: datetime
    ):
        self._identifier = identifier
        self._created_at = created_at
        self._updated_at = created_at
        self._interest_rate_strategy = interest_rate_strategy
        self._initial_amount = balance

        self._balance = balance
        self._interest_payable_balance = Decimal("0")
        self._interest_paid = Decimal("0")

    def get_statement(self, date: datetime):
        self.calculate_interest_payable_balance(date + timedelta(days=1))

        return {
            "identifier": self._identifier,
            "created_at": self._created_at.date().isoformat(),
            "initial_amount": self._initial_amount,
            "balance": self._balance,
            "interest_payable_balance": self._interest_payable_balance,
            "interest_paid": self._interest_paid,
        }

    def calculate_interest_payable_balance(self, date: datetime) -> None:
        interest_period = date - self._updated_at
        self._interest_payable_balance += self._interest_rate_strategy.calculate(
            self._balance,
            Decimal(str(interest_period.days))
        )
        self._updated_at = date

    def pay(self, amount) -> Decimal:
        """
        Pay advance and return remaining amount.
        """
        if amount <= self._balance:
            self._balance -= amount
            return Decimal("0")

        remaining_amount_applicable_to_future_advances = amount - self._balance
        self._balance = 0
        return remaining_amount_applicable_to_future_advances

    def pay_interest(self, amount, paid_date: datetime) -> Decimal:
        """
        Pay interest and return remaining amount.
        """
        self.calculate_interest_payable_balance(paid_date)

        if amount <= self._interest_payable_balance:
            self._interest_payable_balance -= amount
            self._interest_paid += amount
            return Decimal("0")
        remaining_amount_applicable_to_future_advances = amount - self._interest_payable_balance

        self._interest_paid += self._interest_payable_balance
        self._interest_payable_balance = 0
        return remaining_amount_applicable_to_future_advances


if __name__ == "__main__":
    interface()
