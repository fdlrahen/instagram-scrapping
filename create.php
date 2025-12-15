<?php
require __DIR__ . '/config.php';

function indo_to_utc($text) {
    $bulan = [
        'januari'=>'01','februari'=>'02','maret'=>'03','april'=>'04','mei'=>'05','juni'=>'06',
        'juli'=>'07','agustus'=>'08','september'=>'09','oktober'=>'10','november'=>'11','desember'=>'12'
    ];
    $text = strtolower(trim($text));
    $parts = explode(' ', $text);

    if (count($parts) === 3) {
        $d = $parts[0];
        $m = $bulan[$parts[1]] ?? null;
        $y = $parts[2];
        if ($m) {
            // Format sama seperti yang biasa dipakai Python → nanti di-GAS bisa di-parse
            return sprintf('%04d-%02d-%02d 00:00:00', (int)$y, (int)$m, (int)$d);
        }
    }
    return null;
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {

    $shortcode  = $_POST['shortcode'] ?? '';
    $username   = $_POST['username'] ?? '';
    $caption    = $_POST['caption'] ?? '';
    $event_date = $_POST['event_date'] ?? '';
    $date_utc   = indo_to_utc($event_date);
    $permalink  = $_POST['permalink'] ?? '';
    $gas_status = $_POST['gas_status'] ?? '';

    // --- 1) Insert ke DB dulu ---
    $insert = $pdo->prepare(
        "INSERT INTO ig_posts (
            shortcode, username, caption, event_date, date_utc,
            permalink, gas_status, created_at, updated_at
        ) VALUES (
            :shortcode, :username, :caption, :event_date, :date_utc,
            :permalink, :gas_status, NOW(), NOW()
        )"
    );

    $insert->execute([
        'shortcode'  => $shortcode,
        'username'   => $username,
        'caption'    => $caption,
        'event_date' => $event_date,
        'date_utc'   => $date_utc,
        'permalink'  => $permalink,
        'gas_status' => $gas_status,
    ]);

    // Mau langsung kirim ke GAS?
    $send_now = isset($_POST['send_now']);

    if ($send_now) {
        // Samakan bentuk payload dengan yang diharapkan doPost(e)
        // date_utc di-GAS selama ini kamu pakai epoch (detik), jadi kita ikutkan
        $date_epoch = $date_utc ? strtotime($date_utc) : null;

        $record = [
            'shortcode'  => $shortcode,
            'username'   => $username,
            'caption'    => $caption,
            'event_date' => $event_date,
            'date_utc'   => $date_epoch,
            'permalink'  => $permalink,
            // kalau createEventFromRecord_ butuh field lain, tinggal tambahkan di sini
        ];

        $payload = [
            'secret'  => GAS_SECRET,
            'records' => [ $record ],
        ];

        $ch = curl_init(GAS_WEBAPP_URL);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload));
        curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true); // penting, GAS suka redirect ke googleusercontent

        $result   = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err      = curl_error($ch);
        curl_close($ch);

        // Kalau CURL-nya sendiri error
        if ($result === false || $err) {
            header("Location: index.php?status=error&msg=" . urlencode("CURL_ERROR: $err"));
            exit;
        }

        // HTTP status bukan 200 → biasanya URL / deploy salah
        if ($httpCode !== 200) {
            header("Location: index.php?status=error&msg=" . urlencode("HTTP_$httpCode RESP: $result"));
            exit;
        }

        // Cek JSON dari GAS: doPost(e) kamu balikin {ok:true, results:[...]}
        $json = json_decode($result, true);
        if (!$json || empty($json['ok'])) {
            header("Location: index.php?status=error&msg=" . urlencode("INVALID_RESPONSE: $result"));
            exit;
        }

        // Kalau mau, cek juga satu per satu results
        // misal: kalau cuma satu record, cek index 0
        if (!empty($json['results'][0]['ok'])) {
            // update status di DB: gas_sent_at + gas_status
            $upd = $pdo->prepare(
                "UPDATE ig_posts
                 SET gas_sent_at = NOW(), gas_status = 'SENT'
                 WHERE shortcode = :shortcode"
            );
            $upd->execute(['shortcode' => $shortcode]);

            header("Location: index.php?status=sent");
            exit;
        } else {
            $errMsg = $json['results'][0]['error'] ?? 'Unknown error';
            header("Location: index.php?status=error&msg=" . urlencode("GAS_ERROR: $errMsg"));
            exit;
        }
    }

    // Kalau tidak dicentang "kirim sekarang", hanya simpan ke DB
    header("Location: index.php?status=saved");
    exit;
}
?>

<!doctype html>
<html lang="en">
<head>
    <title>Edit Event</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
        rel="stylesheet"
    />
</head>
<body class="bg-light">
<div class="container py-4">
    <h1 class="h4 mb-3">Edit Event Instagram</h1>

    <form method="post" class="card p-3">
        <div class="mb-3">
            <label class="form-label">Shortcode</label>
            <input type="text" name="shortcode" class="form-control" placeholder="contoh: DQqfJ6FiXPO">
        </div>
        <div class="mb-3">
            <label class="form-label">Nama Kegiatan</label>
            <input type="text" name="username" class="form-control">
        </div>

        <div class="mb-3">
            <label class="form-label">Caption</label>
            <input type="text" name="caption" class="form-control">
        </div>

        <div class="mb-3">
            <label class="form-label">Event Date (teks)</label>
            <input type="text" name="event_date" class="form-control" placeholder="contoh: 30 November 2025">
        </div>

        <div class="mb-3">
            <label class="form-label">Permalink</label>
            <input type="text" name="permalink" class="form-control">
        </div>

        <div class="mb-3">
            <label class="form-label">GAS Status</label>
            <input type="text" name="gas_status" class="form-control" placeholder="misal: SENT / PENDING">
        </div>

        <div class="d-flex gap-2">
            <button type="submit" class="btn btn-primary">Simpan</button>
            <a href="index.php" class="btn btn-secondary">Kembali</a>
        </div>
         <div class="mb-3 form-check">
            <input type="checkbox" name="send_now" value="1" class="form-check-input" id="sendNow">
            <label class="form-check-label" for="sendNow">Kirim ke Google Calendar sekarang</label>
        </div>
        
    </form>
</div>
</body>
</html>
