import os
import sys

from configparser import ConfigParser
from datetime import datetime
from db_handler import DBHandler
from jira_interface import JIRAInterface
from subprocess import Popen
from utils import TimestampUtil


class DataQAQCAutoRunHandler:
    def __init__(self):
        hostname, user, auth, qaqc_db_name, jira_db_name = \
            self._get_db_handler_params()
        jira_params = self._get_jira_params()
        self.jira_reporter, self.jira_host, self.jira_issue_path = jira_params
        self.qaqc_db_handler = DBHandler(hostname, user, auth, qaqc_db_name)
        self.jira_db_handler = DBHandler(hostname, user, auth, jira_db_name)
        self.jira_interfacer = JIRAInterface()
        self.ts_util = TimestampUtil()

    def _get_db_handler_params(self):
        with open(os.path.join(os.getcwd(), 'qaqc.cfg')) as cfg:
            return self._parse_db_cfg_section(cfg)

    def _get_jira_params(self):
        with open(os.path.join(os.getcwd(), 'qaqc.cfg')) as cfg:
            return self._parse_jira_cfg_section(cfg)

    def _parse_db_cfg_section(self, cfg):
        hostname = user = auth = None
        qaqc_db_name = jira_db_name = None

        config = ConfigParser()
        config.read_file(cfg)
        cfg_section = 'DB'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'flux_hostname'):
                hostname = config.get(cfg_section, 'flux_hostname')
            if config.has_option(cfg_section, 'flux_user'):
                user = config.get(cfg_section, 'flux_user')
            if config.has_option(cfg_section, 'flux_auth'):
                auth = config.get(cfg_section, 'flux_auth')
            if config.has_option(cfg_section, 'jira_db_name'):
                jira_db_name = config.get(cfg_section, 'jira_db_name')
            if config.has_option(cfg_section, 'flux_db_name'):
                qaqc_db_name = config.get(cfg_section, 'flux_db_name')

        return (hostname, user, auth, qaqc_db_name, jira_db_name)

    def _parse_jira_cfg_section(self, cfg):
        reporter = jira_host = jira_issue_path = None
        config = ConfigParser()
        config.read_file(cfg)
        cfg_section = 'JIRA'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'amf_data_team_reporter'):
                reporter = config.get(cfg_section, 'amf_data_team_reporter')
            if config.has_option(cfg_section, 'jira_host'):
                jira_host = config.get(cfg_section, 'jira_host')
            if config.has_option(cfg_section, 'jira_issue_path'):
                jira_issue_path = config.get(cfg_section, 'jira_issue_path')

        return reporter, jira_host, jira_issue_path

    def get_data_qaqc_candidates(self, qaqc_issues, last_base_gen_time_lookup):
        data_qaqc_wait_state = 'Attempt Data QAQC'
        auto_data_qaqc_ok_states = (
            'Attempt Data QAQC', 'Canceled', 'Replace with Upload')
        auto_run_candidates = {}
        need_review_candidates = {}
        manual_labels = ('BASE', 'FLX-CA')

        for site_id, issues in qaqc_issues.items():
            timestamp_sorted_issues = sorted(
                issues, key=lambda k: k.get('last_create'), reverse=True)
            last_BASE_ts = last_base_gen_time_lookup.get(site_id)
            if last_BASE_ts:
                last_BASE_ts = datetime.strptime(
                    last_BASE_ts, self.ts_util.JIRA_TS_FORMAT)
            if not timestamp_sorted_issues:
                continue

            issue = timestamp_sorted_issues.pop(0)
            issue_num = int(issue.get('issue_num'))
            qaqc_status = issue.get('qaqc_status')
            issue_timestamp = issue.get('last_create')
            issue_label = issue.get('label')

            # ToDo: First check if latest issue is Format QAQC
            if (qaqc_status != data_qaqc_wait_state or
                (last_BASE_ts is not None and
                 issue_timestamp < last_BASE_ts) or
                    issue_label in manual_labels):
                continue
            if any(issue.get('qaqc_status') not in auto_data_qaqc_ok_states
                   for issue in timestamp_sorted_issues):
                reason = ('Has at least one historical QAQC status not '
                          f'in {auto_data_qaqc_ok_states}')
                need_review_candidates[site_id] = {
                    'issue_num': issue_num, 'reason': reason}
                continue

            if (timestamp_sorted_issues and
                (all(issue.get('label') == manual_labels[0]
                     for issue in timestamp_sorted_issues) or
                 all(issue.get('label') == manual_labels[-1]
                     for issue in timestamp_sorted_issues))):
                reason = ('Has all historical QAQC states as  '
                          f'one of the following labels f{manual_labels}')
                need_review_candidates[site_id] = {
                    'issue_num': issue_num,
                    'reason': reason}
                continue
            auto_run_candidates[site_id] = issue_num

        return auto_run_candidates, need_review_candidates

    def exec_data_qaqc(self, site_id, issue_num):
        potential_res_lookup = self.get_potential_res()
        res = potential_res_lookup.get(site_id)
        screen_name = 'data_qaqc_run_' + str(issue_num)
        exe = sys.executable
        exe_cmd = f'{exe} main.py {site_id} {res}\n'
        Popen(['screen', '-dmS', screen_name])
        Popen(['screen', '-rS', screen_name, '-X', 'stuff', exe_cmd])

    def get_potential_res(self):
        last_upload_res_lookup = {}
        last_upload_lookup = self.qaqc_db_handler.get_last_flux_upload()
        for site_id, fnames in last_upload_lookup.items():
            res = []
            for fname in fnames:
                if '_HH_' in fname:
                    res.append('HH')
                elif '_HR_' in fname:
                    res.append('HR')
                else:
                    res.append('None')
            if len(set(res)) > 1:
                continue
            last_upload_res_lookup[site_id] = res.pop()
        return last_upload_res_lookup

    def _process_auto_run_candidates(
            self, auto_run_candidates, in_process_sites):
        auto_run_db_entries = []
        qaqced_issues = self.qaqc_db_handler.get_auto_data_qaqc_runs()
        for site_id, issue_num in auto_run_candidates.items():
            if (site_id in in_process_sites.keys() and
                    (datetime.now() - in_process_sites.get(site_id)).days < 2):
                db_entry = self._create_db_entry(
                    issue_num, 'WARNING', 'Run in progress')
                auto_run_db_entries.append(db_entry)
                continue
            if issue_num in qaqced_issues:
                continue
            self.exec_data_qaqc(site_id, issue_num)
            db_entry = self._create_db_entry(issue_num, 'SUCCESS', '')
            auto_run_db_entries.append(db_entry)
        return auto_run_db_entries

    def _process_need_review_candidates(self, need_review_candidates):
        need_review_db_entries = []
        reviewed_issues = self.qaqc_db_handler.get_auto_data_qaqc_reviews()
        for site_id, review_data in need_review_candidates.items():
            issue_num = review_data.get('issue_num')
            reason = review_data.get('reason')
            if issue_num in reviewed_issues:
                continue
            jira_summary = f'Auto Data QAQC needs review for {site_id}'
            jira_msg = self._create_jira_message(issue_num, reason)
            notification_issue_num = self.jira_interfacer.create_site_issue(
                site_id, self.jira_reporter, jira_summary, jira_msg, [])
            issue_link = self._create_jira_link(notification_issue_num)
            reason += f'. See issue {issue_link}'
            db_entry = self._create_db_entry(issue_num, 'REVIEW', reason)
            need_review_db_entries.append(db_entry)
        return need_review_db_entries

    def _create_jira_link(self, issue_num, project='QAQC'):
        ''' Note: issue_num can be an int or str that contains the project name
        like <JIRA_PROJECT>-<ISSUE_NUMBER>. If that happens, <PROJECT>
        and <ISSUE_NUMBER> will be split apart and used to create the link.'''

        if isinstance(issue_num, str) and '-' in issue_num:
            project, issue_num = issue_num.split('-')
        issue_num = int(issue_num)
        return f'{self.jira_host}{self.jira_issue_path}{project}-{issue_num}'

    def _create_jira_message(self, issue_num, reason):
        reason = reason[0].lower() + reason[1:]
        issue_link = self._create_jira_link(issue_num)
        return f'Issue {issue_num} ({issue_link}) {reason}'

    def _create_db_entry(self, issue_num, state, comment):
        return (issue_num, state, datetime.now(), comment)

    def _log_to_db(self, auto_run_entries, need_review_entries):
        entries = auto_run_entries + need_review_entries
        self.qaqc_db_handler.log_auto_data_qaqc_run(datetime.now(), entries)

    def main(self):
        last_base_gen_time_lookup = \
            self.qaqc_db_handler.get_last_base_gen_time()
        qaqc_issues = self.jira_db_handler.get_qaqc_results()
        candidates, need_review = self.get_data_qaqc_candidates(
            qaqc_issues, last_base_gen_time_lookup)
        in_process_sites = \
            self.qaqc_db_handler.get_data_qaqc_in_process_sites()
        for k, v in in_process_sites.items():
            in_process_sites[k] = datetime.strptime(
                v, self.ts_util.JIRA_TS_FORMAT)

        auto_run_db_entries = self._process_auto_run_candidates(
            candidates, in_process_sites)
        need_review_db_entries = self._process_need_review_candidates(
            need_review)
        self._log_to_db(auto_run_db_entries, need_review_db_entries)


if __name__ == '__main__':
    DataQAQCAutoRunHandler().main()
