@import gg.jte.Content
@import org.huebert.iotfsdb.api.ui.service.BasePageService
@import java.util.Map

@param BasePageService.BasePage basePage
@param String title
@param Map<String, Object> model

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>iotfsdb - Login</title>
    <link rel="icon" href="/icons/cpu.svg" sizes="any" type="image/svg+xml">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.8/css/bootstrap.min.css" integrity="sha512-2bBQCjcnw658Lho4nlXJcc6WkV/UxpE/sAokbXPxQNGqmNdQrWqtw26Ns9kFF/yG792pKR1Sx8/Y1Lf1XN4GKA==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <style>
        body {
            background-color: #f8f9fa;
        }
        .login-container {
            max-width: 400px;
            margin: 100px auto;
            padding: 20px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .login-header {
            text-align: center;
            margin-bottom: 30px;
        }
        .login-header h2 {
            color: #343a40;
        }
        .form-control:focus {
            border-color: #86b7fe;
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h2><i class="bi bi-key"></i> Login to iotfsdb</h2>
            <p>Enter your credentials to access the system</p>
        </div>
        
        @if(model.get("error") != null)
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
                Invalid username or password
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        @endif
        
        @if(model.get("logout") != null)
            <div class="alert alert-success alert-dismissible fade show" role="alert">
                You have been logged out successfully
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        @endif
        
        <form action="/ui/login" method="post">
            <div class="mb-3">
                <label for="username" class="form-label">Username</label>
                <input type="text" class="form-control" id="username" name="username" required>
            </div>
            <div class="mb-3">
                <label for="password" class="form-label">Password</label>
                <input type="password" class="form-control" id="password" name="password" required>
            </div>
            <button type="submit" class="btn btn-primary w-100">Login</button>
        </form>
        
        <div class="mt-3 text-center">
            <small class="text-muted">Powered by iotfsdb ${basePage.getVersion()}</small>
        </div>
    </div>
    
    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.8/js/bootstrap.bundle.min.js" integrity="sha512-HvOjJrdwNpDbkGJIG2ZNqDlVqMo77qbs4Me4cah0HoDrfhrbA+8SBlZn1KrvAQw7cILLPFJvdwIgphzQmMm+Pw==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
</body>
</html>