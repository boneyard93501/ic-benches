# test_bucket_ic_eu_safe.yml — robust ic-eu bucket-creation test with non-fatal logging

- name: Safe bucket-creation test for ic-eu
  hosts: bench
  gather_facts: yes
  vars:
    repo_path: /opt/ic-benches
    log_file: "{{ repo_path }}/bucket_test_ic_eu.log"
    tmp_http_endpoint: ""

  tasks:
    - name: Load .env vars if available
      shell: |
        set -a
        [ -f "{{ repo_path }}/.env" ] && . "{{ repo_path }}/.env"
        set +a
        env
      register: env_vars
      changed_when: false

    - name: Parse config.toml for ic-eu provider
      delegate_to: localhost
      shell: |
        python3 - <<'PYCODE'
        import tomllib, json
        cfg = tomllib.load(open("config.toml","rb"))
        ic = next(p for p in cfg["providers"] if p["id"]=="ic-eu")
        print(json.dumps(ic))
        PYCODE
      args:
        chdir: "{{ playbook_dir }}/.."
      register: ic_provider
      changed_when: false

    - name: Set provider facts
      set_fact:
        ic_conf: "{{ ic_provider.stdout | from_json }}"
        endpoint: "{{ (ic_provider.stdout | from_json).endpoint }}"
        namespace: "{{ (ic_provider.stdout | from_json).namespace }}"
        bucket: "{{ (ic_provider.stdout | from_json).bucket }}"
        insecure: "{{ (ic_provider.stdout | from_json).insecure_ssl | default(false) }}"

    - name: Show provider info
      debug:
        msg: "Testing bucket {{ bucket }} on {{ endpoint }} (namespace={{ namespace }}, insecure_ssl={{ insecure }})"

    - name: Try HTTPS bucket creation first
      shell: |
        set -e
        mc alias rm {{ namespace }} >/dev/null 2>&1 || true
        if [ "{{ insecure }}" = "true" ]; then
          mc alias set {{ namespace }} {{ endpoint }} "$ACCESS_KEY" "$SECRET_KEY" --api S3v4 --insecure
          mc mb --ignore-existing --insecure {{ namespace }}/{{ bucket }}
        else
          mc alias set {{ namespace }} {{ endpoint }} "$ACCESS_KEY" "$SECRET_KEY" --api S3v4
          mc mb --ignore-existing {{ namespace }}/{{ bucket }}
        fi
      register: https_result
      ignore_errors: yes

    - name: If HTTPS fails, derive HTTP endpoint
      set_fact:
        tmp_http_endpoint: "{{ endpoint | regex_replace('^https', 'http') }}"
      when: https_result.rc != 0

    - name: Retry with HTTP (if previous failed)
      shell: |
        set -e
        if [ -n "{{ tmp_http_endpoint }}" ]; then
          mc alias rm {{ namespace }} >/dev/null 2>&1 || true
          mc alias set {{ namespace }} {{ tmp_http_endpoint }} "$ACCESS_KEY" "$SECRET_KEY" --api S3v4 --insecure
          mc mb --ignore-existing --insecure {{ namespace }}/{{ bucket }}
        fi
      register: http_result
      when: https_result.rc != 0
      ignore_errors: yes

    - name: Summarize results
      set_fact:
        final_rc: "{{ 0 if https_result.rc == 0 else (http_result.rc | default(1)) }}"
        final_stdout: "{{ https_result.stdout if https_result.rc == 0 else (http_result.stdout | default('')) }}"
        final_stderr: "{{ https_result.stderr if https_result.rc == 0 else (http_result.stderr | default('')) }}"

    - name: Write detailed log to remote file
      copy:
        dest: "{{ log_file }}"
        content: |
          Timestamp: {{ ansible_date_time.iso8601 }}
          Endpoint: {{ endpoint }}
          HTTP fallback: {{ tmp_http_endpoint | default('none') }}
          Namespace: {{ namespace }}
          Bucket: {{ bucket }}
          Insecure SSL: {{ insecure }}
          Result code: {{ final_rc }}
          --- HTTPS stdout ---
          {{ https_result.stdout | default('') }}
          --- HTTPS stderr ---
          {{ https_result.stderr | default('') }}
          --- HTTP stdout ---
          {{ http_result.stdout | default('') if http_result is defined else '' }}
          --- HTTP stderr ---
          {{ http_result.stderr | default('') if http_result is defined else '' }}

    - name: Print concise outcome
      debug:
        msg: >
          {% if final_rc == 0 %}
          ✅ SUCCESS: Bucket {{ bucket }} created or exists on {{ endpoint }}
          {% else %}
          ⚠️  FAILED: Could not create bucket {{ bucket }} (check {{ log_file }})
          {% endif %}
