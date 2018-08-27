;;; kafka-cli.el --- Summary
;;; Commentary:
;;; Code:

;; todo check if library exists else issue warning
(eval-when-compile 
  (require 'magit-popup)
  (require 'cl-lib))
;; (require 'kafka-cli-sections)
(require 'comint)

;;; Custom variables

(defgroup kafka-cli nil
  "Kafka CLI"
  :group 'utilities
  :prefix "kafka-")

(defcustom kafka-cli-bin-path "/home/ebby/apps/kafka/kafka/bin/"
  "Kafka CLI tools path."
  :type 'string
  :group 'kafka-cli)

(defcustom kafka-cli-config-path "/home/ebby/apps/kafka/kafka/config/"
  "Kafka CLI config path."
  :type 'string
  :group 'kafka-cli)

(defcustom kafka-zookeeper-url "localhost:2181"
  "Zookeeper hostname and port."
  :type 'string
  :group 'kafka-cli)

(defcustom kafka-url "localhost:9092"
  "Kafka broker hostname and port."
  :type 'string
  :group 'kafka-cli)

(defvar kafka-verbosity-level 1 "Kafka emits messages if non-nil")

;; -------------------------------------------------------------------
(autoload 'kafka-run-zookeeper "kafka-cli-servieces")
(autoload 'kafka-run-consumer "kafka-cli-servieces")
(autoload 'kafka-run-broker "kafka-cli-services")
(autoload 'kafka-restart-consumer "kafka-cli-servieces")
(autoload 'kafka-services-running "kafka-cli-services")
(autoload 'kafka-cli-section-goto-topic "kafka-cli-sections")
(autoload 'consumer-desc-section-toggle "kafka-cli-sections")
(autoload 'topic-desc-section-toggle "kafka-cli-sections")

(defun kafka-bin (bin)
  (expand-file-name bin kafka-cli-bin-path))

(defun kafka-config (config)
  (expand-file-name config kafka-cli-config-path))

(cl-defmacro kafka-buffer (&optional service &key proc name out)
    "Standardizes buffer naming for service buffers - optional returns 
buffer process, name, or buffer-name with '-output- appended."
    `(,(if name 'identity (if proc 'get-buffer-process 'get-buffer-create))
      ,(concat "*" (pcase service
                     ((or 'zookeeper "zookeeper") "zookeeper")
                     ((or 'broker "broker") "broker")
                     ((or 'topics "topics") "kafka-topics")
                     ((or 'consumer "consumer") "consumer")
                     (_ "kafka"))
               (and out "-output") "*")))

(eval-when-compile
  (defmacro kafka-buffer-proc (service)
    "buffer process object"
    (kafka-buffer service :proc t))

  (defmacro kafka-emit (fmt &rest args)
    (when kafka-verbosity-level
      `(message ,fmt ,@args)))

  (defmacro with-output-to-topics-list (bin args &rest body)
    (declare (indent 2))
    `(let* ((buff (kafka-buffer nil :out t))
            (call-proc-args (list (kafka-bin ,bin) nil buff t))
            (kafka-args ,args)
            (option-args
             (apply 'append
                    (mapcar 'split-string (kafka-create-alter-topics-arguments)))))
       (apply 'call-process (append call-proc-args kafka-args option-args))
       ,@body
       (kafka-topics-list))))

;;;###autoload
(defun kafka-topics-alter (topic)
  "Alter topic TOPIC."
  (interactive (list (completing-read "Topic:" (kafka--get-topics))))
  (with-output-to-topics-list "kafka-topics.sh"
    (list "--zookeeper" kafka-zookeeper-url
          "--alter"
          "--topic" topic)
    (kafka-emit "Topic: %s, altered" topic)))

;;;###autoload
(defun kafka-topics-create (topic partition)
  "Create the TOPIC with PARTITION."
  (interactive "sTopic: \nsPartition:")
  (with-output-to-topics-list "kafka-topics.sh"
    (list "--zookeeper" kafka-zookeeper-url
          "--create"
          "--topic" topic
          "--partition" partition
          "--replication-factor" "1")
    (kafka-emit "Topic: %s, created" topic)))

;;;###autoload
(defun kafka-topics-delete (topic)
  "Delete the TOPIC ."
  (interactive (list (completing-read "Topic:" (kafka--get-topics))))
  (call-process (kafka-bin "kafka-topics.sh") nil (kafka-buffer topics) t
                  "--zookeeper" kafka-zookeeper-url
                  "--topic" topic
                  "--delete")
  (kafka-emit "Topic: %s, deleted" topic)
  (kafka-topics-list))

;;;###autoload
(defun kafka-topics-describe (topic)
  "Describe the topic TOPIC in the kafka-topics section."
  (interactive (list (completing-read "Topic:" (kafka--get-topics))))
  (kafka-topics-list)
  (kafka-cli-section-goto-topic topic)
  (kafka-topics-describe-at-point))

;;;###autoload
(defun kafka-topics-list ()
  "List all the topics in the zookeeper."
  (interactive) ;; Refer magit how to write your own list buffer mode?
  (let* ((buff (kafka-buffer nil :out t))
	 (topics-cli (kafka-bin "kafka-topics.sh")))
    (set-buffer buff)
    (setq buffer-read-only 'nil)
    (erase-buffer)
    (call-process topics-cli nil buff t
                  "--zookeeper" kafka-zookeeper-url
                  "--list")
    (switch-to-buffer buff)
    (kafka-cli-topic-mode)))

(defvar kafka--all-topics)
(defun kafka--get-topics (&optional update)
  "Either get topics or get and update based on flag UPDATE."
  (if (or (not kafka--all-topics) update)
      (save-excursion
        (kafka-topics-list)
        (with-current-buffer (kafka-buffer topics)
	  (setq kafka--all-topics (split-string (buffer-string)))))
    kafka--all-topics))

(defun kafka-consumer-get-offset (topic)
  "Get TOPIC offset information."
  (kafka-emit "topic: %S" topic)
  (let* ((consumer-cli (kafka-bin "kafka-consumer-offset-checker.sh"))
	 (output
          (process-lines consumer-cli
                         "--topic" topic
                         "--zookeeper" kafka-zookeeper-url
                         "--group" "kafka-cli-consumer"))
	 (keys (split-string (cadr output)))
	 (values (split-string (caddr output))))
    (cl-mapcar #'cons keys values)))

(defun kafka-consumer-describe-at-point ()
  "."
  (interactive)
  (let* ((topic (buffer-substring-no-properties (line-beginning-position)
                                                (line-end-position)))
	 (output (kafka-consumer-get-offset topic)))
    (save-excursion
      (consumer-desc-section-toggle output))))

(defun kafka-topics-delete-at-point ()
  "Delete topic at point."
  (interactive)
  (let* ((topic (buffer-substring-no-properties (line-beginning-position)
                                                (line-end-position))))
    (if (yes-or-no-p (format "Delete topic: %s" topic))
	(kafka-topics-delete topic)
      (kafka-emit "(No deletions performed)"))))

;; move some raw formatting of output from sections to here.
(defun kafka-topic-get-desc (topic)
  "Describe topic TOPIC."
  (process-lines (kafka-bin "kafka-topics.sh")
                 "--topic" topic
                 "--zookeeper" kafka-zookeeper-url
                 "--describe"))

(defun kafka-topics-describe-at-point ()
  "."
  (interactive)
  (let* ((topic (buffer-substring-no-properties (line-beginning-position)
                                                (line-end-position))))
    (save-excursion
      (topic-desc-section-toggle (kafka-topic-get-desc topic)))))

(defun kafka-show-server ()
  "Show Kafka Server."
  (interactive)
  (kafka-run-broker 1)
  (kafka-cli-log-mode))

(defun kafka-show-zk-server ()
  "Show Zookeeper Buffer."
  (interactive)
  (kafka-run-zookeeper 1)
  (kafka-cli-log-mode))

(defun kafka-show-consumer ()
  "Show Consumer Buffer."
  (interactive)
  (kafka-run-consumer 1)
  (kafka-cli-consumer-mode))

(defun kafka-restart-consumer-wrapper ()
  "Restart Consumer"
  (interactive)
  (kafka-restart-consumer 1))

(defun kafka-show-all-services ()
  "Show all buffers FIXME load the mode."
  (interactive)
  (kafka-run-broker 'nil)
  (kafka-run-zookeeper 'nil)
  (kafka-run-consumer 'nil)
  (display-buffer-in-side-window (kafka-buffer) '((side . bottom) (slot . -3)))
  (display-buffer-in-side-window (kafka-buffer zookeeper)
                                 '((side . bottom) (slot . -2)))
  (display-buffer-in-side-window (kafka-buffer consumer)
                                 '((side . bottom) (slot . -1))))

(magit-define-popup kafka-create-alter-topics-popup
  "Kafka Create Topics"
  :options '((?p "[compact, delete]" "--config cleanup.policy=")
	     (?z "[uncompressed, snappy, lz4, gzip, producer]"
                 "--config compression.type=")
	     (?x "[0,...]" "--config delete.retention.ms=")
	     (?X "[0,...]" "--config file.delete.delay.ms=")
	     (?f "[0,...]" "--config flush.messages=")
	     (?F "[0,...]" "--config flush.ms=")
	     (?T "kafka.server.ThrottledReplicaListValidator$@1060b431"
                 "--config follower.replication.throttled.="))
  :actions '((?c "Create Topic" kafka-topics-create)
	     (?a "Alter Topic" kafka-topics-alter)
	     (?q "Back" bury-buffer))
  :default-action 'kafka-topics-create)

;; rename aptly
(magit-define-popup kafka-topics-options-popup
  "Kafka Topics Options"
  :actions '((?c "Describe consumer for topic"
                 kafka-consumer-describe-at-point)
	     (?d "Describe Topic" kafka-topics-describe-at-point)
	     (?q  "Back" bury-buffer))
  :default-action 'bury-buffer)

;;;###autoload(autoload 'kafka-topics-popup "kafka-cli" nil t)
(magit-define-popup kafka-topics-popup
  "Kafka Topics Popup."
  :actions '((?c "Create/Alter Topics"
                 kafka-create-alter-topics-popup)
	     (?d "Delete Topics" kafka-topics-delete)
	     (?h "Describe Topics" kafka-topics-describe)
	     (?O "Services Overview" kafka-services-popup)
	     (?l "List all Topics" kafka-topics-list)
	     (?q "Back" bury-buffer))
  :default-action 'kafka-topics-list)

(defun do-nothing ()
  (message "not implemented"))

;;;###autoload(autoload 'kafka-consumer-popup "kafka-cli" nil t)
(magit-define-popup kafka-consumer-popup
  "Kafka Topics Popup."
  :actions '((?R "Restart Consumer" kafka-restart-consumer-wrapper)
	     (?P "Pause Consumer" pause-kafkaconsumer)
	     (?C "Continue Consumer" continue-kafkaconsumer)
	     (?q "Back/Bury Buffer" bury-buffer))
  :default-action 'kafka-topics-list)


(defun kafka-cli ()
  "Start the kafka services and displays the popup."
  (interactive)
  (if (kafka-services-running)
      (kafka-topics-popup)
    (let ((msg (eval-when-compile
                 (concat "Kafka, Zk services are not started."
		         "`kafka-run-zookeeper', `kafka-run-broker',"
                         "`kafka-run-consumer'"))))
      (message "%s" msg)
      (display-warning :error msg))))

(magit-define-popup kafka-services-popup
  "Some doc"
  :actions '((?z "View Zookeeper" kafka-show-zk-server)
	     (?k "View Kafka" kafka-show-server)
	     (?c "View Consumer Status" kafka-show-consumer)
	     (?A "View All Services" kafka-show-all-services))
  :default-action 'kafka-show-server)

(defvar kafka-cli-log-mode-map
  (let ((map (make-keymap)))
    (define-key map (kbd "q") 'bury-buffer) map)
  "Keymap for `kafka-cli-log-mode'.")

;; use rx and improvise this
(defvar kafka-cli-log-mode-highlights
  '((("INFO\\|WARN"
      . font-lock-keyword-face)
     ("^\\[\\(.*?\\)\\]" . font-lock-builtin-face)
     ("\(\\(.*?\\)\)" . font-lock-variable-name-face))))

(define-derived-mode kafka-cli-log-mode comint-mode "KafkaCliLog"
  "Mode for looking at kafka services.
\\{kafka-cli-log-mode-map}"
  :group 'kafka-topics
  (use-local-map kafka-cli-log-mode-map)
  (setq font-lock-defaults kafka-cli-log-mode-highlights)
  (setq buffer-read-only 'nil))

(defvar kafka-cli-consumer-mode-map
  (let ((map (make-keymap)))
    (define-key map  (kbd "q") 'bury-buffer)
    (define-key map  (kbd "?") 'kafka-consumer-popup)
    map)
  "Keymap for `kafka-cli-consumer-mode' .")

(defvar kafka-cli-consumer-mode-highlights
  '((("\\w*" . font-lock-variable-name-face))))

(define-derived-mode kafka-cli-consumer-mode comint-mode "KafkaCliConsumer"
  "Mode for looking at consumer.
\\{kafka-cli-consumer-mode-map}"
  :group 'kafka-topics
  (use-local-map kafka-cli-consumer-mode-map)
  (setq font-lock-defaults kafka-cli-consumer-mode-highlights)
  (setq buffer-read-only 'nil))

(defvar kafka-cli-topic-mode-map
  (let ((map (make-keymap)))
    (define-key map (kbd "?") 'kafka-topics-popup)
    (define-key map (kbd "q") 'bury-buffer)
    map)
  "Keymap for `kafka-cli-topic-mode'.")

(defvar kafka-cli-topic-highlights
  (eval-when-compile
    (let ((keywords
           (regexp-opt
            '("Broker" "Configs" "Group" "Isr" "Leader" "logSize" "Lag" "Offset"
              "Owner" "PartitionCount" "Partition" "Pid" "Replicas"
              "ReplicationFactor" "Topic"))))
      `(((,keywords                            . font-lock-keyword-face)
         ("\\w*"                               . font-lock-variable-name-face)
         (":\\|,\\|;\\|{\\|}\\|=>\\|@\\|$\\|=" . font-lock-string-face))))))

;; Clean this up
(defun kafka-cli-topic-mode-properties ()
  "."
  (interactive)
  (let* ((more-lines t)
	 (map (make-sparse-keymap))
	 (start)
	 (end))
    (with-current-buffer (kafka-buffer topics)
      (define-key map (kbd "C-m") 'kafka-topics-describe-at-point)
      (define-key map (kbd "C-o") 'kafka-consumer-describe-at-point)
      (define-key map (kbd "?") 'kafka-topics-options-popup)
      (define-key map (kbd "D") 'kafka-topics-delete-at-point)
      (goto-char (point-min))
      (while more-lines
	(setq start (line-beginning-position))
	(setq end (line-end-position))
	(put-text-property start end 'keymap map)
	(add-text-properties start end '(topic t))
	(setq more-lines (= 0 (forward-line 1)))))))

(define-derived-mode kafka-cli-topic-mode special-mode "KafkaCliTopic"
  "Mode for looking at kafka topics.
\\{kafka-cli-topic-mode-map}"
  :group 'kafka-cli-topics
  (use-local-map kafka-cli-topic-mode-map)
  (setq font-lock-defaults kafka-cli-topic-highlights)
  (setq buffer-read-only 'nil)
  (kafka-cli-topic-mode-properties)
  (setq buffer-read-only t))

(provide 'kafka-cli)

;;; kafka-cli.el ends here
