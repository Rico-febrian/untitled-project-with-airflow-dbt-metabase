from airflow.providers.slack.hooks.slack import SlackHook
from airflow.models import Variable


def send_failure_alert(context):
    """
    Callback function that sends a Slack message when any task fails.
    Attached to DAG via on_failure_callback.
    """

    task_instance = context["task_instance"]
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context["logical_date"].strftime("%Y-%m-%d %H:%M")
    log_url = task_instance.log_url

    channel = Variable.get("slack_alert_channel")
    slack_hook = SlackHook(slack_conn_id="slack_conn")

    message = (
        f":red_circle: *Pipeline Failed*\n\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Log:* {log_url}"
    )

    slack_hook.client.chat_postMessage(
        channel=channel,
        text=message,
    )
